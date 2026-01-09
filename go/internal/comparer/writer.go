package comparer

import (
	"fmt"
	"io"
	"os"
)

// WriteDeltaScript writes comparison results to file or stdout
func WriteDeltaScript(results []*ComparisonResult, outputPath string) error {
	var out io.Writer
	if outputPath != "" {
		file, err := os.Create(outputPath)
		if err != nil {
			return err
		}
		defer file.Close()
		out = file
	} else {
		out = os.Stdout
	}

	// Write header
	fmt.Fprintln(out, "-- Full Delta Update Script")
	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 0;")
	fmt.Fprintln(out)

	// Write changes (inserts/updates)
	for _, result := range results {
		if result.Changes != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Changes)
		}
	}

	// Write deletions
	fmt.Fprintln(out, "-- DELETIONS")
	for _, result := range results {
		if result.Deletions != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Deletions)
		}
	}

	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 1;")

	return nil
}
