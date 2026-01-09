package comparer

import (
	"fmt"
	"io"
	"os"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// WriteDeltaScript writes comparison results to file or stdout
func WriteDeltaScript(results []*ComparisonResult, outputPath string, p *mpb.Progress) error {
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

	var bar *mpb.Bar
	if p != nil {
		bar = p.New(
			int64(len(results)*2), // *2 for changes and deletions passes
			mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding(" ").Rbound("]"),
			mpb.PrependDecorators(
				decor.Name("Writing delta script", decor.WC{W: 20, C: decor.DindentRight | decor.DextraSpace}),
				decor.CountersNoUnit("%d / %d", decor.WC{W: 18, C: decor.DindentRight | decor.DextraSpace}),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 5}),
			),
		)
	}

	// Write changes (inserts/updates)
	for _, result := range results {
		if result.Changes != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Changes)
		}
		if bar != nil {
			bar.IncrBy(1)
		}
	}

	// Write deletions
	fmt.Fprintln(out, "-- DELETIONS")
	for _, result := range results {
		if result.Deletions != "" {
			fmt.Fprintf(out, "-- TABLE %s\n", result.TableName)
			fmt.Fprint(out, result.Deletions)
		}
		if bar != nil {
			bar.IncrBy(1)
		}
	}

	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 1;")
	if bar != nil {
		bar.SetTotal(int64(len(results)*2), true)
	}

	return nil
}
