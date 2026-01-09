package comparer

import (
	"fmt"
	"io"
	"os"

	"github.com/schollz/progressbar/v3"
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

	// Create progress bar for writing
	bar := progressbar.NewOptions(len(results)*2, // *2 for changes and deletions passes
		progressbar.OptionSetDescription("Writing delta script"),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
	)
	defer bar.Close()

	// Write changes (inserts/updates)
	for _, result := range results {
		if result.Changes != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Changes)
		}
		bar.Add(1)
	}

	// Write deletions
	fmt.Fprintln(out, "-- DELETIONS")
	for _, result := range results {
		if result.Deletions != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Deletions)
		}
		bar.Add(1)
	}

	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 1;")

	return nil
}
