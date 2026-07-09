package tools

import (
	"path"
	"strings"
)

const defaultEtcdPrefix = "/testp"

func CleanEtcdPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return defaultEtcdPrefix
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return path.Clean(prefix)
}
