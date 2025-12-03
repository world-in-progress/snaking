package internal

import (
	"sync"
)

var (
	globalMinIOClient *MinIOClient
	once              sync.Once
)

func initGlobalClient() error {
	var err error
	once.Do(func() {
		config := loadMinIOConfigFromEnv()
		globalMinIOClient, err = NewMinIOClient(config)
	})
	return err
}

func loadMinIOConfigFromEnv() MinIOConfig {
	return MinIOConfig{
		Endpoint:        GetEnv("MINIO_ENDPOINT"),
		AccessKeyID:     GetEnv("MINIO_ACCESS_KEY_ID"),
		SecretAccessKey: GetEnv("MINIO_SECRET_ACCESS_KEY"),
		UseSSL:          GetEnvAsBool("MINIO_USE_SSL", false),
	}
}

func GetGlobalClient() *MinIOClient {
	if err := initGlobalClient(); err != nil {
		panic("failed to initialize global MinIO client: " + err.Error())
	}
	return globalMinIOClient
}
