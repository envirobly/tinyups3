package main

import (
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

type partUpload struct {
	partNumber int32
	data       []byte
	size       int
}

type uploadResult struct {
	part types.CompletedPart
	err  error
}

// Zero-allocation bytes reader that implements io.ReadSeeker
type bytesReader struct {
	data []byte
	pos  int64
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data, pos: 0}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += int64(n)
	return n, nil
}

func (r *bytesReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = r.pos + offset
	case io.SeekEnd:
		newPos = int64(len(r.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	
	if newPos < 0 {
		return 0, errors.New("negative position")
	}
	
	r.pos = newPos
	return newPos, nil
}

func main() {
	// Parse command line flags
	partSizeMB := flag.Int("partSize", 5, "Part size in MB for multipart upload (min 5MB)")
	inputSize := flag.Int64("inputSize", 0, "Exact input size in bytes (required)")
	concurrency := flag.Int("concurrency", 1, "Number of concurrent part uploads")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [--partSize=MB] [--inputSize=bytes] [--concurrency=N] s3://bucket/key\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Validate arguments
	if *partSizeMB < 5 {
		log.Fatalf("partSize must be at least 5MB")
	}
	if *inputSize <= 0 {
		log.Fatalf("inputSize must be a positive integer")
	}
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	if *concurrency < 1 {
		*concurrency = 1
	}

	// Parse S3 URI
	s3uri := flag.Arg(0)
	bucket, key, err := parseS3URI(s3uri)
	if err != nil {
		log.Fatalf("Error parsing S3 URI: %v", err)
	}

	// Initialize AWS client
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Error loading AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = false
		o.EndpointResolver = s3.EndpointResolverFromURL(
			fmt.Sprintf("https://s3.dualstack.%s.amazonaws.com", cfg.Region),
		)
	})

	partSize := *partSizeMB * 1024 * 1024

	// Calculate exact number of parts needed
	partsCount := int(*inputSize / int64(partSize))
	if *inputSize%int64(partSize) != 0 {
		partsCount++
	}

	log.Printf("Starting upload: %d parts, %d MB each, %d concurrent workers", 
		partsCount, *partSizeMB, *concurrency)

	// Start multipart upload
	createOutput, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("Failed to initiate multipart upload: %v", err)
	}
	uploadID := createOutput.UploadId

	// Memory-efficient buffer pool - only allocate what we need
	bufferPool := &sync.Pool{
		New: func() interface{} {
			buf := make([]byte, partSize)
			return &buf
		},
	}

	// Channels for coordination
	partsChan := make(chan partUpload, *concurrency)
	resultsChan := make(chan uploadResult, *concurrency)
	
	// Context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start worker goroutines
	var workerWG sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		workerWG.Add(1)
		go func(workerID int) {
			defer workerWG.Done()
			
			for part := range partsChan {
				// Create reader from the part data
				reader := newBytesReader(part.data[:part.size])
				
				// Upload the part
				uploadInput := &s3.UploadPartInput{
					Bucket:     &bucket,
					Key:        &key,
					UploadId:   uploadID,
					PartNumber: &part.partNumber,
					Body:       reader,
				}
				
				result := uploadResult{}
				uploadOutput, err := client.UploadPart(ctx, uploadInput)
				
				// Return buffer to pool immediately after upload
				bufferPool.Put(&part.data)
				
				if err != nil {
					result.err = err
				} else {
					result.part = types.CompletedPart{
						ETag:       uploadOutput.ETag,
						PartNumber: &part.partNumber,
					}
				}
				
				// Send result back
				select {
				case resultsChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Start result collector goroutine
	completedParts := make([]types.CompletedPart, partsCount)
	var collectorWG sync.WaitGroup
	var uploadError error
	
	collectorWG.Add(1)
	go func() {
		defer collectorWG.Done()
		
		for i := 0; i < partsCount; i++ {
			select {
			case result := <-resultsChan:
				if result.err != nil {
					uploadError = result.err
					cancel() // Cancel all workers
					return
				}
				// Store completed part in correct position
				idx := int(*result.part.PartNumber) - 1
				if idx >= 0 && idx < len(completedParts) {
					completedParts[idx] = result.part
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Read data from stdin and dispatch to workers
	var partNumber int32 = 1
	var readError error
	
	for partNumber <= int32(partsCount) {
		// Get buffer from pool
		bufPtr := bufferPool.Get().(*[]byte)
		buf := *bufPtr
		
		// Calculate how much to read for this part
		remainingBytes := *inputSize - int64(partNumber-1)*int64(partSize)
		readSize := int64(partSize)
		if remainingBytes < readSize {
			readSize = remainingBytes
		}
		
		// Read data from stdin
		n, err := io.ReadFull(os.Stdin, buf[:readSize])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			bufferPool.Put(bufPtr) // Return buffer on error
			readError = fmt.Errorf("failed to read part %d: %w", partNumber, err)
			break
		}
		
		if n == 0 {
			bufferPool.Put(bufPtr) // Return buffer if no data read
			break
		}
		
		// Create upload task
		task := partUpload{
			partNumber: partNumber,
			data:       buf,
			size:       n,
		}
		
		// Send to workers (this will block if all workers are busy)
		select {
		case partsChan <- task:
			// Task dispatched successfully
		case <-ctx.Done():
			bufferPool.Put(bufPtr)
			break
		}
		
		partNumber++
		
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	
	// Close parts channel and wait for workers to finish
	close(partsChan)
	workerWG.Wait()
	
	// Wait for result collector to finish
	collectorWG.Wait()
	
	// Check for errors
	if readError != nil {
		abortMultipart(ctx, client, bucket, key, uploadID)
		log.Fatalf("Read error: %v", readError)
	}
	
	if uploadError != nil {
		abortMultipart(ctx, client, bucket, key, uploadID)
		log.Fatalf("Upload error: %v", uploadError)
	}
	
	// Complete the multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abortMultipart(ctx, client, bucket, key, uploadID)
		log.Fatalf("Failed to complete multipart upload: %v", err)
	}
	
	// Force garbage collection to free any remaining memory
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
