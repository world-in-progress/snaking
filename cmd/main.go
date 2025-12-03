package main

import (
	"context"
	"fmt"

	"snaking/internal"

	"github.com/minio/minio-go/v7"
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

	minioConfig := internal.MinIOConfig{
		Endpoint:        "local-minio:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
	}
	client, err := internal.NewMinIOClient(minioConfig)
	if err != nil {
		fmt.Printf("Error creating MinIO client: %v\n", err)
		return
	}

	ctx := context.Background()
	if err = client.Pull(ctx, "snaking", "earth.jpg", minio.GetObjectOptions{}); err != nil {
		fmt.Printf("Error pulling file from MinIO: %v\n", err)
		return
	}

	if err = client.Pull(ctx, "snaking", "raw/1.txt", minio.GetObjectOptions{}); err != nil {
		fmt.Printf("Error pulling file from MinIO: %v\n", err)
		return
	}

	if err = client.Pull(ctx, "snaking", "raw/", minio.GetObjectOptions{}); err != nil {
		fmt.Printf("Error pulling folder from MinIO: %v\n", err)
		return
	}

	fmt.Println("Successfully pulled folder from MinIO")
}
