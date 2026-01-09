package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/vbauerster/mpb/v8"
	"github.com/younes/sqldumpdiff/internal/comparer"
	"github.com/younes/sqldumpdiff/internal/logger"
	"github.com/younes/sqldumpdiff/internal/parser"
)

func main() {
	// Parse command line arguments
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Configure logging
	if *debug {
		logger.SetLogLevel(logger.DebugLevel)
	} else {
		logger.SetLogLevel(logger.InfoLevel)
	}

	args := flag.Args()
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [--debug] old_dump.sql new_dump.sql [delta.sql]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  If delta.sql is not specified, output goes to stdout\n")
		os.Exit(1)
	}

	oldFile := args[0]
	newFile := args[1]
	deltaFile := ""
	if len(args) > 2 {
		deltaFile = args[2]
	}

	logger.Debug("main: Starting SQL dump diff")
	logger.Debug("main: Old file: %s", oldFile)
	logger.Debug("main: New file: %s", newFile)
	logger.Debug("main: Delta file: %s", deltaFile)

	// Parse schemas to get primary keys and columns (in parallel with mpb)
	schemaParser := parser.NewSchemaParser()

	// Create progress container for multi-bar display
	p := mpb.New(mpb.WithOutput(os.Stderr))

	type schemaRes struct {
		pkMap map[string][]string
		err   error
	}

	type colsRes struct {
		cols map[string][]string
		err  error
	}

	oldSchemaCh := make(chan schemaRes, 1)
	newSchemaCh := make(chan schemaRes, 1)
	oldColsCh := make(chan colsRes, 1)
	newColsCh := make(chan colsRes, 1)

	// Parse schemas in parallel
	go func() {
		pk, err := schemaParser.ParseSchemas(oldFile, p)
		oldSchemaCh <- schemaRes{pkMap: pk, err: err}
	}()

	go func() {
		pk, err := schemaParser.ParseSchemas(newFile, p)
		newSchemaCh <- schemaRes{pkMap: pk, err: err}
	}()

	// Parse columns in parallel
	go func() {
		cols, err := schemaParser.ParseColumns(oldFile, p)
		oldColsCh <- colsRes{cols: cols, err: err}
	}()

	go func() {
		cols, err := schemaParser.ParseColumns(newFile, p)
		newColsCh <- colsRes{cols: cols, err: err}
	}()

	// Wait for results
	oldSchemaRes := <-oldSchemaCh
	if oldSchemaRes.err != nil {
		log.Fatalf("Error parsing old schema: %v", oldSchemaRes.err)
	}
	oldPKMap := oldSchemaRes.pkMap

	newSchemaRes := <-newSchemaCh
	if newSchemaRes.err != nil {
		log.Fatalf("Error parsing new schema: %v", newSchemaRes.err)
	}
	newPKMap := newSchemaRes.pkMap

	oldColsRes := <-oldColsCh
	if oldColsRes.err != nil {
		log.Fatalf("Error parsing old columns: %v", oldColsRes.err)
	}
	oldColsMap := oldColsRes.cols

	newColsRes := <-newColsCh
	if newColsRes.err != nil {
		log.Fatalf("Error parsing new columns: %v", newColsRes.err)
	}
	newColsMap := newColsRes.cols

	// Merge PK maps (prefer new file PKs)
	for table, pk := range newPKMap {
		oldPKMap[table] = pk
	}

	// Generate delta
	dg := comparer.NewDeltaGenerator(oldPKMap, oldColsMap, newColsMap)
	results, err := dg.GenerateDelta(oldFile, newFile, p)
	if err != nil {
		log.Fatalf("Error generating delta: %v", err)
	}

	// Print summary
	totalInserts := 0
	totalUpdates := 0
	totalDeletes := 0

	for _, result := range results {
		totalInserts += result.InsertCount
		totalUpdates += result.UpdateCount
		totalDeletes += result.DeleteCount
	}

	fmt.Printf("Summary:\n")
	fmt.Printf("  Inserts: %d\n", totalInserts)
	fmt.Printf("  Updates: %d\n", totalUpdates)
	fmt.Printf("  Deletes: %d\n", totalDeletes)
	fmt.Printf("  Total:   %d\n", totalInserts+totalUpdates+totalDeletes)

	// Write delta script
	if err := comparer.WriteDeltaScript(results, deltaFile, p); err != nil {
		log.Fatalf("Error writing delta script: %v", err)
	}

	// Ensure progress bars are fully rendered before exit
	p.Wait()

	logger.Debug("main: Delta generation complete")
}
