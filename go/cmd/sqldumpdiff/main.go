package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/vbauerster/mpb/v8"
	"github.com/younes/sqldumpdiff/internal/comparer"
	"github.com/younes/sqldumpdiff/internal/logger"
	"github.com/younes/sqldumpdiff/internal/parser"
	"gopkg.in/yaml.v3"
)

type sqliteProfilesFile struct {
	Profiles map[string]sqliteProfile `yaml:"profiles"`
}

type sqliteProfile struct {
	CacheKB int `yaml:"cache_kb"`
	MmapMB  int `yaml:"mmap_mb"`
	Batch   int `yaml:"batch"`
	Workers int `yaml:"workers"`
}

func loadSQLiteProfiles(path string) (map[string]sqliteProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg sqliteProfilesFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.Profiles) == 0 {
		return nil, fmt.Errorf("no profiles found in %s", path)
	}
	return cfg.Profiles, nil
}

func profileKeys(m map[string]sqliteProfile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func main() {
	// Parse command line arguments
	debug := flag.Bool("debug", false, "Enable debug logging")
	sqliteCacheKB := flag.Int("sqlite-cache-kb", 800000, "SQLite cache size in KB (negative means KB)")
	sqliteMmapMB := flag.Int("sqlite-mmap-mb", 128, "SQLite mmap size in MB")
	sqliteBatch := flag.Int("sqlite-batch", 20000, "SQLite insert batch size")
	sqliteWorkers := flag.Int("sqlite-workers", 0, "Max concurrent table compares (0 = NumCPU)")
	sqliteProfile := flag.String("sqlite-profile", "fast", "SQLite tuning profile: low-mem, balanced, fast")
	sqliteProfileFile := flag.String("sqlite-profile-file", "sqlite_profiles.yaml", "SQLite profiles YAML file")
	flag.Parse()

	// Configure logging
	if *debug {
		logger.SetLogLevel(logger.DebugLevel)
	} else {
		logger.SetLogLevel(logger.InfoLevel)
	}

	profiles, err := loadSQLiteProfiles(*sqliteProfileFile)
	if err != nil {
		log.Fatalf("Failed to load SQLite profiles: %v", err)
	}
	profile, ok := profiles[*sqliteProfile]
	if !ok {
		log.Fatalf("Invalid --sqlite-profile value: %s (available: %v)", *sqliteProfile, profileKeys(profiles))
	}
	comparer.ConfigureSQLiteTunables(profile.CacheKB, profile.MmapMB, profile.Batch, profile.Workers)

	comparer.ConfigureSQLiteTunables(*sqliteCacheKB, *sqliteMmapMB, *sqliteBatch, *sqliteWorkers)

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
	var out *os.File
	if deltaFile != "" {
		file, err := os.Create(deltaFile)
		if err != nil {
			log.Fatalf("Error creating delta file: %v", err)
		}
		defer file.Close()
		out = file
	} else {
		out = os.Stdout
	}

	summary, err := dg.GenerateDelta(oldFile, newFile, p, out)
	if err != nil {
		log.Fatalf("Error generating delta: %v", err)
	}

	// Ensure progress bars are fully rendered before printing summary
	p.Wait()

	// Print summary (Java-style)
	sep := strings.Repeat("=", 60)
	fmt.Printf("\n%s\nSUMMARY\n%s\n", sep, sep)
	fmt.Printf("Inserts:  %d\n", summary.InsertCount)
	fmt.Printf("Updates:  %d\n", summary.UpdateCount)
	fmt.Printf("Deletes:  %d\n", summary.DeleteCount)
	fmt.Printf("Total:    %d\n", summary.InsertCount+summary.UpdateCount+summary.DeleteCount)
	fmt.Printf("%s\n", sep)

	logger.Debug("main: Delta generation complete")
}
