package meta

import (
	"fmt"
	"regexp"
	"strings"
)

var indexNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// CheckIndexName validates the shared index naming rules without loading a runtime.
func CheckIndexName(name string) error {
	if name == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if strings.HasPrefix(name, "_") {
		return fmt.Errorf("index name cannot start with _")
	}
	if !indexNameRe.MatchString(name) {
		return fmt.Errorf("index name [%s] is invalid, just accept [a-zA-Z0-9_.-]", name)
	}
	return nil
}
