package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
}

func DownloadFromMinIO(ctx context.Context, config MinIOConfig, objectName string) (string, error) {
	// Initialize MinIO client
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return "", fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	baseName := filepath.Base(objectName)
	localFilePath := filepath.Join("/tmp/minio", baseName)
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(localFilePath), 0777); err != nil {
		return "", fmt.Errorf("failed to create directory for %s: %w", localFilePath, err)
	}

	// Download the object
	err = minioClient.FGetObject(ctx, config.BucketName, objectName, localFilePath, minio.GetObjectOptions{})
	if err != nil {
		os.Remove(localFilePath)
		return "", fmt.Errorf("failed to download object %s: %w", objectName, err)
	}

	return localFilePath, nil
}
