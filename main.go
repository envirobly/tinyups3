package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

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
	partSizeMB := flag.Int("partSize", 64, "Part size in MB for multipart upload (min 5MB)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [--partSize=MB] s3://bucket/key\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Validate part size
	if *partSizeMB < 5 {
		log.Fatalf("partSize must be at least 5MB")
	}
	partSize := *partSizeMB * 1024 * 1024

	// Validate args
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

	// Enable dualstack endpoint
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
	var parts []types.CompletedPart
	reader := bufio.NewReaderSize(os.Stdin, partSize)

	partNumber := int32(1)
	buffer := make([]byte, partSize)

	for {
		n, err := io.ReadFull(reader, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			abortMultipart(ctx, client, bucket, key, uploadID)
			log.Fatalf("Failed to read input: %v", err)
		}

		if n == 0 {
			break
		}

		pn := partNumber // local variable to take address of
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
		log.Printf("Uploaded part %d (%d bytes)", partNumber, n)

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
