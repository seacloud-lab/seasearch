package vector

import (
	"fmt"
	"os"
)

// Disk SaveFile moves the temporary file, whereas object backends may leave it.
func removeMaintenanceFile(name string) error {
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove maintenance temporary file %s: %w", name, err)
	}
	return nil
}
