package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func parseS3URI(s3uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3uri, "s3://") {
		return "", "", errors.New("invalid S3 URI: must start with s3://")
	}
	u, err := url.Parse(s3uri)
	if err != nil {
		return "", "", err
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if bucket == "" || key == "" {
		return "", "", errors.New("invalid S3 URI: missing bucket or key")
	}
	return bucket, key, nil
}

func main() {
	// Define flags
	partSizeMB := flag.Int("partSize", 5, "Part size in MB for multipart upload (min 5MB)")
	inputSize := flag.Int64("inputSize", 0, "Exact input size in bytes (required)")
	concurrency := flag.Int("concurrency", 1, "Number of concurrent part uploads (min 1)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [--partSize=MB] [--inputSize=bytes] [--concurrency=N] s3://bucket/key\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Validate flags
	if *partSizeMB < 5 {
		log.Fatalf("partSize must be at least 5MB")
	}
	if *concurrency < 1 {
		log.Fatalf("concurrency must be at least 1")
	}
	partSize := *partSizeMB * 1024 * 1024
	if *inputSize <= 0 {
		log.Fatalf("inputSize must be a positive integer")
	}
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	s3uri := flag.Arg(0)
	bucket, key, err := parseS3URI(s3uri)
	if err != nil {
		log.Fatalf("Error parsing S3 URI: %v", err)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Error loading AWS config: %v", err)
	}

	// Configure client with dualstack endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = false
		o.EndpointResolver = s3.EndpointResolverFromURL(
			fmt.Sprintf("https://s3.dualstack.%s.amazonaws.com", cfg.Region),
		)
	})

	// Start multipart upload
	createOutput, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("Failed to initiate multipart upload: %v", err)
	}
	uploadID := createOutput.UploadId

	// Calculate exact number of parts
	partsCount := int(*inputSize/int64(partSize)) + 1
	if *inputSize%int64(partSize) == 0 {
		partsCount--
	}

	// Channel for part upload tasks
	type partTask struct {
		partNumber int32
		data       []byte
	}
	partChan := make(chan partTask, partsCount)
	completedParts := make([]types.CompletedPart, 0, partsCount)
	var partsMu sync.Mutex // Mutex for thread-safe parts slice
	var wg sync.WaitGroup
	errChan := make(chan error, *concurrency)

	// Start worker pool
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range partChan {
				partInput := &s3.UploadPartInput{
					Bucket:     &bucket,
					Key:        &key,
					UploadId:   uploadID,
					PartNumber: &task.partNumber,
					Body:       bytes.NewReader(task.data),
				}

				uploadOutput, err := client.UploadPart(ctx, partInput)
				if err != nil {
					errChan <- fmt.Errorf("failed to upload part %d: %v", task.partNumber, err)
					return
				}

				partsMu.Lock()
				completedParts = append(completedParts, types.CompletedPart{
					ETag:       uploadOutput.ETag,
					PartNumber: &task.partNumber,
				})
				partsMu.Unlock()
			}
		}()
	}

	// Read input and dispatch parts
	partNumber := int32(1)
	buffer := make([]byte, partSize)
	for {
		n, err := io.ReadAtLeast(os.Stdin, buffer, partSize)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			close(partChan)
			abortMultipart(ctx, client, bucket, key, uploadID)
			log.Fatalf("Failed to read input: %v", err)
		}

		if n == 0 {
			break
		}

		// Create a copy of the buffer for this part
		partData := make([]byte, n)
		copy(partData, buffer[:n])

		partChan <- partTask{
			partNumber: partNumber,
			data:       partData,
		}

		partNumber++
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	close(partChan)

	// Wait for all uploads to complete
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		abortMultipart(ctx, client, bucket, key, uploadID)
		log.Fatalf("Upload error: %v", err)
	default:
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		log.Fatalf("Failed to complete multipart upload: %v", err)
	}

	// Clean up memory
	buffer = nil
	runtime.GC()

	log.Println("Upload completed successfully.")
}

func abortMultipart(ctx context.Context, client *s3.Client, bucket, key string, uploadID *string) {
	_, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: uploadID,
	})
	if err != nil {
		log.Printf("Warning: failed to abort multipart upload: %v", err)
	}
}
