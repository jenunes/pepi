package ftdc

import (
	"context"
	"strings"
)

// StreamBatches is a public wrapper to stream ftdc metrics
func StreamBatches(ctx context.Context, path string, metricsIncludeFilePath string, batchSize, buffer int) (<-chan StreamBatch, <-chan error) {
	return streamFTDCMetricsInBatches(ctx, path, metricsIncludeFilePath, batchSize, buffer)
}

func ReadMetadata(ctx context.Context, path string) (map[string]interface{}, error) {
	return readMetadata(ctx, path)
}

func GetTags(ctx context.Context, path string) (map[string]string, error) {
	metadata, err := ReadMetadata(ctx, path)
	if err != nil {
		return map[string]string{}, ErrInvalidFormat
	}

	if metadata == nil || metadata["doc"] == nil {
		return map[string]string{}, ErrInvalidFormat
	}

	hostname := getNestedString(metadata, "doc.hostInfo.system.hostname")

	if hostname == "" {
		hostname = getNestedString(metadata, "doc.common.hostInfo.system.hostname")
	}
	version := getNestedString(metadata, "doc.buildInfo.version")

	if version == "" {
		version = getNestedString(metadata, "doc.common.buildInfo.version")

	}
	return map[string]string{
		"hostname": hostname,
		"version":  version,
	}, nil
}

func getNestedString(m map[string]interface{}, path string) string {
	keys := strings.Split(path, ".")
	var curr interface{} = m

	for _, k := range keys {
		next, ok := curr.(map[string]interface{})
		if !ok {
			return ""
		}
		curr, ok = next[k]
		if !ok {
			return ""
		}
	}

	val, ok := curr.(string)
	if !ok {
		return ""
	}
	return val
}
