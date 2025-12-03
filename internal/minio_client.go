package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/sync/errgroup"
)

type (
	MinIOConfig struct {
		Endpoint        string
		AccessKeyID     string
		SecretAccessKey string
		UseSSL          bool
	}

	MinIOClient struct {
		Config MinIOConfig
		Client *minio.Client
	}

	PullBucketBatch struct {
		BucketName string
		Objects    []struct {
			ObjectName string
			GetOptions minio.GetObjectOptions
		}
	}
)

func NewMinIOClient(config MinIOConfig) (*MinIOClient, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	return &MinIOClient{
		Config: config,
		Client: client,
	}, nil
}

func (m *MinIOClient) HasBucket(ctx context.Context, bucketName string) (bool, error) {
	if exists, err := m.Client.BucketExists(ctx, bucketName); err != nil {
		return false, fmt.Errorf("failed to check if bucket %s exists: %w", bucketName, err)
	} else {
		return exists, nil
	}
}

func (m *MinIOClient) pullSingleFile(ctx context.Context, bucketName, objectName string, getOptions minio.GetObjectOptions) error {
	localPath := filepath.Join("/tmp/minio", bucketName, objectName)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", localPath, err)
	}

	if err := m.Client.FGetObject(ctx, bucketName, objectName, localPath, getOptions); err != nil {
		return fmt.Errorf("failed to download object %s: %w", objectName, err)
	}

	return nil
}

func (m *MinIOClient) PullBatch(ctx context.Context, batch []PullBucketBatch) error {
	g, groupCtx := errgroup.WithContext(ctx)
	// Get CPU count for concurrency limit
	numCPU := runtime.NumCPU()
	g.SetLimit(max(numCPU/2, 1)) // at least 1 or half of CPUs to avoid overload

	for _, b := range batch {
		for _, obj := range b.Objects {
			g.Go(func() error {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				default:
				}

				return m.pullSingleFile(groupCtx, b.BucketName, obj.ObjectName, obj.GetOptions)
			})
		}
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("something went wrong when pull multi-objects from minio: %w", err)
	}

	return nil
}

func (m *MinIOClient) PullFolder(ctx context.Context, bucketName, prefix string) error {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
		fmt.Printf("Warning: prefix %s does not end with '/', appending it automatically.\n", prefix)
	}

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	var objects []struct {
		ObjectName string
		GetOptions minio.GetObjectOptions
	}

	for object := range m.Client.ListObjects(ctx, bucketName, opts) {
		if object.Err != nil {
			return fmt.Errorf("failed to list object in bucket %s with prefix %s: %w", bucketName, prefix, object.Err)
		}

		// Skip the folder itself
		if object.Key == prefix {
			continue
		}

		objects = append(objects, struct {
			ObjectName string
			GetOptions minio.GetObjectOptions
		}{
			ObjectName: object.Key,
			GetOptions: minio.GetObjectOptions{},
		})
	}

	if len(objects) == 0 {
		return nil
	}

	batch := []PullBucketBatch{
		{
			BucketName: bucketName,
			Objects:    objects,
		},
	}

	return m.PullBatch(ctx, batch)
}

func (m *MinIOClient) Pull(ctx context.Context, bucketName, objectName string, getOptions minio.GetObjectOptions) error {
	// Check if objectName is a folder
	if strings.HasSuffix(objectName, "/") {
		return m.PullFolder(ctx, bucketName, objectName)
	}
	return m.pullSingleFile(ctx, bucketName, objectName, getOptions)
}
