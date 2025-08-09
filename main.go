package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	var (
		concurrency   = flag.Int("concurrency", 1, "Number of concurrent uploads")
		partSizeMB    = flag.Int("part-size", 5, "Part size in MB for multipart upload")
		contentLength = flag.Int64("content-length", -1, "Content length in bytes (required)")
	)
	flag.Parse()

	// Get the S3 URI from command line arguments
	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] s3://bucket/path/to/file\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Require content length
	if *contentLength < 0 {
		fmt.Fprintf(os.Stderr, "Error: --content-length is required\n")
		fmt.Fprintf(os.Stderr, "Example: cat file.txt | %s --content-length=$(wc -c < file.txt) s3://bucket/key\n", os.Args[0])
		os.Exit(1)
	}

	s3URI := args[0]
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		log.Fatalf("Error parsing S3 URI: %v", err)
	}

	// Load AWS configuration with dualstack endpoint
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled),
	)
	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Create uploader with custom configuration
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.Concurrency = *concurrency
		u.PartSize = int64(*partSizeMB * 1024 * 1024) // Convert MB to bytes
	})

	// Upload from stdin
	fmt.Fprintf(os.Stderr, "Uploading stdin to %s (size: %d bytes) with concurrency=%d, part-size=%dMB\n", 
		s3URI, *contentLength, *concurrency, *partSizeMB)

	stdinWrapper := manager.ReadSeekCloser(os.Stdin)

	result, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          stdinWrapper,
		ContentLength: aws.Int64(*contentLength),
	})
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}

	fmt.Fprintf(os.Stderr, "Upload completed successfully to %s\n", result.Location)
}

// parseS3URI parses an S3 URI and returns bucket and key
func parseS3URI(uri string) (bucket, key string, err error) {
	// Match s3://bucket/key pattern
	re := regexp.MustCompile(`^s3://([^/]+)/(.+)$`)
	matches := re.FindStringSubmatch(uri)
	
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid S3 URI format, expected s3://bucket/path/to/file")
	}

	bucket = matches[1]
	key = matches[2]
	
	// Validate bucket name (basic validation)
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name cannot be empty")
	}
	
	// Validate key
	if key == "" {
		return "", "", fmt.Errorf("key cannot be empty")
	}
	
	return bucket, key, nil
}
