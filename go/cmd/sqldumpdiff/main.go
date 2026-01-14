package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/younes/sqldumpdiff/internal/comparer"
	"github.com/younes/sqldumpdiff/internal/logger"
	"github.com/younes/sqldumpdiff/internal/parser"
	"gopkg.in/yaml.v3"
)

// sqliteProfilesFile is the on-disk YAML shape for performance presets.
// It keeps the CLI defaults out of code so we can tweak them without recompiling.
type sqliteProfilesFile struct {
	Profiles map[string]sqliteProfile `yaml:"profiles"`
}

// sqliteProfile represents one preset for SQLite tuning.
// These map directly onto the runtime knobs used by the comparer package.
type sqliteProfile struct {
	CacheKB int `yaml:"cache_kb"`
	MmapMB  int `yaml:"mmap_mb"`
	Batch   int `yaml:"batch"`
	Workers int `yaml:"workers"`
}

// loadSQLiteProfiles reads the YAML file and returns the profile map.
// We keep YAML parsing here (CLI layer) instead of in comparer to keep that
// package focused on diff logic.
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

// profileKeys returns a stable list of profile names for error messages.
func profileKeys(m map[string]sqliteProfile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func main() {
	startWall := time.Now()
	// Parse command line arguments.
	// Each flag maps to a specific runtime tuning knob, and users can override
	// the YAML profile defaults at the command line.
	debug := flag.Bool("debug", false, "Enable debug logging")
	timing := flag.Bool("timing", false, "Emit timing diagnostics even without --debug")
	timingJSON := flag.String("timing-json", "", "Write timing report JSON to file")
	sqliteProfile := flag.String("sqlite-profile", "fast", "SQLite tuning profile: low-mem, balanced, fast")
	sqliteProfileFile := flag.String("sqlite-profile-file", "sqlite_profiles.yaml", "SQLite profiles YAML file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [--debug] [--timing] [--timing-json <file>] old_dump.sql new_dump.sql [delta.sql]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Configure logging early so everything below can use logger.Debug/Info.
	if *debug {
		logger.SetLogLevel(logger.DebugLevel)
	} else {
		logger.SetLogLevel(logger.InfoLevel)
	}

	// Load and apply the named profile from YAML.
	profiles, err := loadSQLiteProfiles(*sqliteProfileFile)
	if err != nil {
		log.Fatalf("Failed to load SQLite profiles: %v", err)
	}
	profile, ok := profiles[*sqliteProfile]
	if !ok {
		log.Fatalf("Invalid --sqlite-profile value: %s (available: %v)", *sqliteProfile, profileKeys(profiles))
	}
	comparer.ConfigureSQLiteTunables(profile.CacheKB, profile.MmapMB, profile.Batch, profile.Workers)
	comparer.ConfigureTiming(*timing)

	// Remaining arguments are positional: old file, new file, and optional output path.
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
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

	// Parse schemas to get primary keys and columns (parallelized for speed).
	startSchema := time.Now()
	schemaParser := parser.NewSchemaParser()

	// Create a single progress container for all concurrent progress bars.
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

	// Parse schemas in parallel (old/new).
	go func() {
		pk, err := schemaParser.ParseSchemas(oldFile, p)
		oldSchemaCh <- schemaRes{pkMap: pk, err: err}
	}()

	go func() {
		pk, err := schemaParser.ParseSchemas(newFile, p)
		newSchemaCh <- schemaRes{pkMap: pk, err: err}
	}()

	// Parse columns in parallel (old/new).
	go func() {
		cols, err := schemaParser.ParseColumns(oldFile, p)
		oldColsCh <- colsRes{cols: cols, err: err}
	}()

	go func() {
		cols, err := schemaParser.ParseColumns(newFile, p)
		newColsCh <- colsRes{cols: cols, err: err}
	}()

	// Wait for results from all four parsing goroutines.
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
	if *timing || logger.IsDebugEnabled() {
		logger.Debug("Timing: schema/columns parsing took %s", time.Since(startSchema))
	}

	// Merge PK maps (prefer new file PKs if both have a PK definition).
	for table, pk := range newPKMap {
		oldPKMap[table] = pk
	}

	// Generate delta and stream output directly to the file/stdout.
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

	summary, timingMeta, err := dg.GenerateDelta(oldFile, newFile, p, out)
	if err != nil {
		log.Fatalf("Error generating delta: %v", err)
	}

	// Ensure progress bars are fully rendered before printing summary text.
	p.Wait()

	// Print summary (Java-style formatting to match the Java tool).
	sep := strings.Repeat("=", 60)
	fmt.Printf("\n%s\nSUMMARY\n%s\n", sep, sep)
	fmt.Printf("Inserts:  %d\n", summary.InsertCount)
	fmt.Printf("Updates:  %d\n", summary.UpdateCount)
	fmt.Printf("Deletes:  %d\n", summary.DeleteCount)
	fmt.Printf("Total:    %d\n", summary.InsertCount+summary.UpdateCount+summary.DeleteCount)
	fmt.Printf("%s\n", sep)

	if *timing || logger.IsDebugEnabled() {
		logger.Debug("Timing: total wall time %s", time.Since(startWall))
	}

	if *timingJSON != "" && timingMeta != nil {
		writeTimingJSON(*timingJSON, time.Since(startSchema), time.Since(startWall), timingMeta)
	}

	logger.Debug("main: Delta generation complete")
}

type timingTable struct {
	Table     string `json:"table"`
	LoadMS    int64  `json:"load_ms"`
	CompareMS int64  `json:"compare_ms"`
	DeleteMS  int64  `json:"delete_ms"`
	TotalMS   int64  `json:"total_ms"`
}

type timingReport struct {
	SchemaMS  int64         `json:"schema_ms"`
	SplitMS   int64         `json:"split_ms"`
	CompareMS int64         `json:"compare_ms"`
	DeleteMS  int64         `json:"delete_ms"`
	WriteMS   int64         `json:"write_ms"`
	WriteFormatMS int64     `json:"write_format_ms"`
	WriteIOMS     int64     `json:"write_io_ms"`
	TotalMS   int64         `json:"total_ms"`
	WallMS    int64         `json:"wall_ms"`
	Tables    []timingTable `json:"tables"`
}

func writeTimingJSON(path string, schemaDur, wallDur time.Duration, meta *comparer.TimingMeta) {
	tables := make([]timingTable, 0, len(meta.Tables))
	for _, t := range meta.Tables {
		tables = append(tables, timingTable{
			Table:     t.Table,
			LoadMS:    t.LoadMS,
			CompareMS: t.CompMS,
			DeleteMS:  t.DelMS,
			TotalMS:   t.TotalMS(),
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].TotalMS > tables[j].TotalMS })

	report := timingReport{
		SchemaMS:  schemaDur.Milliseconds(),
		SplitMS:   meta.SplitMS,
		CompareMS: meta.CompareMS,
		DeleteMS:  meta.DeleteMS,
		WriteMS:   meta.WriteMS,
		WriteFormatMS: meta.WriteFormatMS,
		WriteIOMS: meta.WriteIOMS,
		TotalMS:   schemaDur.Milliseconds() + meta.SplitMS + meta.CompareMS + meta.DeleteMS + meta.WriteMS,
		WallMS:    wallDur.Milliseconds(),
		Tables:    tables,
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		logger.Error("Timing JSON marshal failed: %v", err)
		return
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		logger.Error("Timing JSON write failed: %v", err)
	}
}
