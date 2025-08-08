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
	"syscall"

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

// estimateAllocatableMemory uses syscall.Sysinfo to get available memory on Linux.
// Returns memory in MB or an error if insufficient.
func estimateAllocatableMemory(requiredMB int) (int, error) {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0, fmt.Errorf("failed to get system memory info: %v", err)
	}

	// Calculate available memory (free + buffers) in MB
	availableBytes := info.Freeram + info.Bufferram
	availableMB := int(availableBytes / (1024 * 1024))

	if availableMB < requiredMB {
		return availableMB, fmt.Errorf("insufficient memory. Available: %d MB, Required: %d MB", availableMB, requiredMB)
	}
	return availableMB, nil
}

func main() {
	// Define flags
	inputSize := flag.Int64("inputSize", 0, "Exact input size in bytes (required)")
	requiredMemory := flag.Int("requiredMemory", 128, "Minimum required memory in MB")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [--inputSize=bytes] [--requiredMemory=MB] s3://bucket/key\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Validate input size
	if *inputSize <= 0 {
		log.Fatalf("inputSize must be a positive integer")
	}

	// Validate required memory
	if *requiredMemory <= 0 {
		log.Fatalf("requiredMemory must be a positive integer")
	}

	// Estimate allocatable memory
	availableMB, err := estimateAllocatableMemory(*requiredMemory)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Calculate partSize
	const minPartSizeMB = 5
	const maxPartSizeMB = 5120 // S3 max part size (5GB)
	partSizeMB := minPartSizeMB
	if availableMB > *requiredMemory {
		excessMB := availableMB - *requiredMemory
		partSizeMB += excessMB / 2 // +1MB per 2MB excess
	}
	if partSizeMB > maxPartSizeMB {
		partSizeMB = maxPartSizeMB
	}
	partSize := partSizeMB * 1024 * 1024

	// Log partSize for debugging
	log.Printf("Using partSize: %d MB (based on %d MB available memory, %d MB required)", partSizeMB, availableMB, *requiredMemory)

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

	// Calculate exact number of parts
	estParts := int(*inputSize / int64(partSize))
	if *inputSize%int64(partSize) != 0 {
		estParts++
	}

	// Start multipart upload
	createOutput, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("Failed to initiate multipart upload: %v", err)
	}

	uploadID := createOutput.UploadId
	parts := make([]types.CompletedPart, 0, estParts) // Preallocate slice
	buffer := make([]byte, partSize)                 // Single buffer allocation

	partNumber := int32(1)
	for {
		// Read directly from os.Stdin to avoid bufio.Reader overhead
		n, err := io.ReadAtLeast(os.Stdin, buffer, partSize)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			abortMultipart(ctx, client, bucket, key, uploadID)
			log.Fatalf("Failed to read input: %v", err)
		}

		if n == 0 {
			break
		}

		pn := partNumber // Local variable for address
		partInput := &s3.UploadPartInput{
			Bucket:     &bucket,
			Key:        &key,
			UploadId:   uploadID,
			PartNumber: &pn,
			Body:       bytes.NewReader(buffer[:n]),
		}

		uploadOutput, err := client.UploadPart(ctx, partInput)
		if err != nil {
			abortMultipart(ctx, client, bucket, key, uploadID)
			log.Fatalf("Failed to upload part %d: %v", partNumber, err)
		}

		parts = append(parts, types.CompletedPart{
			ETag:       uploadOutput.ETag,
			PartNumber: &pn,
		})

		partNumber++
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		log.Fatalf("Failed to complete multipart upload: %v", err)
	}

	// Clean up memory
	buffer = nil
	parts = nil
	runtime.GC() // Optional: trigger GC for constrained systems

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
