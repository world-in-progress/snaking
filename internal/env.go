package internal

import (
	"fmt"
	"os"
	"strconv"
)

func GetEnv(key string, fallback ...string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	if len(fallback) > 0 {
		return fallback[0]
	}

	panic(fmt.Sprintf("Environment variable %s is not set and is required (no default fallback proveded).", key))
}

func GetEnvAsBool(key string, fallback ...bool) bool {
	valStr, exists := os.LookupEnv(key)
	if !exists {
		if len(fallback) > 0 {
			return fallback[0]
		}
		panic(fmt.Sprintf("Environment variable %s is not set and is required (no default fallback proveded).", key))
	}

	if val, err := strconv.ParseBool(valStr); err != nil {
		panic(fmt.Sprintf("Environment variable %s value %s cannot be parsed as bool: %v", key, valStr, err))
	} else {
		return val
	}
}
