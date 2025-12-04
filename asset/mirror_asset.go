package asset

import (
	"context"
	"fmt"
	"snaking/internal"
	"strings"

	"github.com/minio/minio-go/v7"
)

type MirrorAsset struct {
	Path     string
	isFolder bool
	client   *internal.MinIOClient
}

func NewMirrorAsset(path string) (*MirrorAsset, error) {
	client := internal.GetGlobalClient()
	isFolder := strings.HasSuffix(path, "/")

	// Check if the bucket exists
	bucketName := strings.SplitN(path, "/", 2)[0]
	ctx := context.Background()
	if exists, err := client.HasBucket(ctx, bucketName); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucketName)
	}

	return &MirrorAsset{
		Path:     path,
		isFolder: isFolder,
		client:   internal.GetGlobalClient(),
	}, nil
}

func (m *MirrorAsset) Where() string {
	local_base := internal.GetEnv("LOCAL_BASE_PATH", "tmp/minio/")
	return local_base + m.Path
}

func (m *MirrorAsset) Pull() error {
	parts := strings.SplitN(m.Path, "/", 2)
	bucketName := parts[0]
	objectName := parts[1]

	ctx := context.Background()
	if err := m.client.Pull(ctx, bucketName, objectName, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("failed to pull asset %s from bucket %s: %w", objectName, bucketName, err)
	}
	return nil
}

func (m *MirrorAsset) ShareTo(shmName string) error {
	if m.isFolder {
		return fmt.Errorf("cannot share a folder asset")
	}

	if err := internal.CopyFileToShm(m.Where(), shmName); err != nil {
		return err
	}
	return nil
}
