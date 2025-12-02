package main

import (
	"context"
	"fmt"
	"log"
	"snaking/internal"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	fmt.Println("Hello, Snaking!")

	// Execute the MinIO interaction test
	// if err := interactWithMinio(); err != nil {
	// 	log.Printf("Error interacting with MinIO: %v", err)
	// }

	tryToLoad()
}

func tryToLoad() {
	ctx := context.Background()
	config := internal.MinIOConfig{
		Endpoint:        "local-minio:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
		BucketName:      "snaking",
	}
	objectName := "earth.jpg"
	if _, err := internal.DownloadFromMinIO(ctx, config, objectName); err != nil {
		log.Printf("Error downloading from MinIO: %v", err)
	}
}

func interactWithMinio() error {
	ctx := context.Background()

	// Configuration for MinIO
	endpoint := "local-minio:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	// Initialize MinIO client object
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}

	// List all buckets to test connection
	fmt.Println("Successfully connected to MinIO. Listing buckets:")

	buckets, err := minioClient.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	for _, bucket := range buckets {
		fmt.Println(bucket.Name)
	}

	// Create a test bucket if it doesn't exist
	bucketName := "snaking-test-bucket"
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		// Create the bucket
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		fmt.Printf("Bucket %s created successfully\n", bucketName)
	} else {
		fmt.Printf("Bucket %s already exists\n", bucketName)
	}

	return nil
}
