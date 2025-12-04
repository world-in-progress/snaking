package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShareFile(t *testing.T) {
	// Create a temporary source file
	tmpDir := t.TempDir()
	sourceContent := []byte("Hello, Shared Memory!")
	sourcePath := filepath.Join(tmpDir, "source.txt")

	err := os.WriteFile(sourcePath, sourceContent, 0644)
	require.NoError(t, err, "Failed to create source file")

	// Define test cases
	tests := []struct {
		name        string
		shmName     string
		expectError bool
	}{
		{
			name:        "Simple file name",
			shmName:     "test_shm_simple",
			expectError: false,
		},
		{
			name:        "Nested path",
			shmName:     "nested/test_shm_nested",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Cleanup shm file after test
			shmPath := filepath.Join(ShmPath, tt.shmName)
			defer func() {
				os.Remove(shmPath)
				// Only try to remove dir if it is a subdirectory, not ShmPath itself
				dir := filepath.Dir(shmPath)
				if dir != filepath.Clean(ShmPath) {
					os.Remove(dir)
				}
			}()

			// 3. Execute
			err := CopyFileToShm(sourcePath, tt.shmName)

			// 4. Verify
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify file exists in /dev/shm
				assert.FileExists(t, shmPath)

				// Verify content matches
				content, err := os.ReadFile(shmPath)
				assert.NoError(t, err)
				assert.Equal(t, sourceContent, content, "Shared memory content should match source")
			}
		})
	}
}

func TestShareFile_SourceNotFound(t *testing.T) {
	err := CopyFileToShm("/non/existent/path", "test_shm_fail")
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err), "Error should be file not found")
}
