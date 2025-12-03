package internal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

// TestMinIOClient is an integration test
// A local MinIO server must be running for this test to work
// Make sure local-minio:9000 is accessible with minioadmin:minioadmin credentials
func TestMinIOClient_Pull(t *testing.T) {
	config := MinIOConfig{
		Endpoint:        "local-minio:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSSL:          false,
	}
	bucketName := "snaking-test-pull"
	ctx := context.Background()

	client, err := NewMinIOClient(config)
	if err != nil {
		t.Skipf("Skipping test, could not connect to MinIO: %v", err)
		return
	}

	setupTestEnv(t, ctx, client.Client, bucketName)
	defer cleanupTestEnv(t, ctx, client.Client, bucketName)

	tests := []struct {
		name          string
		objectName    string
		expectedError bool
		checkFile     bool
		expectedLocal string
	}{
		{
			name:          "Pull single file",
			objectName:    "test-file.txt",
			expectedError: false,
			checkFile:     true,
			expectedLocal: "/tmp/minio/snaking-test-pull/test-file.txt",
		},
		{
			name:          "Pull file in folder",
			objectName:    "folder/file_1.txt",
			expectedError: false,
			checkFile:     true,
			expectedLocal: "/tmp/minio/snaking-test-pull/folder/file_1.txt",
		},
		{
			name:          "Pull folder",
			objectName:    "folder/",
			expectedError: false,
			checkFile:     true,
			expectedLocal: "/tmp/minio/snaking-test-pull/folder/file_50.txt", // check one of the files in the folder
		},
		{
			name:          "Pull non-existent file",
			objectName:    "non-existent-file.txt",
			expectedError: true,
			checkFile:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Pull(ctx, bucketName, tt.objectName, minio.GetObjectOptions{})

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.checkFile {
				require.FileExists(t, tt.expectedLocal)
			}
		})
	}
}

func setupTestEnv(t *testing.T, ctx context.Context, client *minio.Client, bucket string) {
	exists, err := client.BucketExists(ctx, bucket)
	require.NoError(t, err)
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	// Add folders and files
	files := make(map[string]string, 51)
	files["test-file.txt"] = "Hello world"
	for i := 1; i <= 50; i++ {
		objectName := "folder/file_" + fmt.Sprintf("%d", i) + ".txt"
		files[objectName] = "This is file number " + fmt.Sprintf("%d", i)
	}

	for name, content := range files {
		data := []byte(content)
		_, err = client.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
		require.NoError(t, err)
	}
}

func cleanupTestEnv(t *testing.T, ctx context.Context, client *minio.Client, bucket string) {
	objectsCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive: true,
	})
	for obj := range objectsCh {
		err := client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			t.Logf("Failed to remove object %s: %v", obj.Key, err)
		}
	}

	err := client.RemoveBucket(ctx, bucket)
	if err != nil {
		t.Logf("Failed to remove bucket %s: %v", bucket, err)
	}

	// Clean up local files
	err = os.RemoveAll("/tmp/minio/" + bucket)
	if err != nil {
		t.Logf("Failed to remove local temp files: %v", err)
	}
}
