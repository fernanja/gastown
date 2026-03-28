package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

func TestOutputMoleculeStatus_StandaloneFormulaShowsVars(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir tempDir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	status := MoleculeStatusInfo{
		HasWork:         true,
		PinnedBead:      &beads.Issue{ID: "gt-wisp-xyz", Title: "Standalone formula work"},
		AttachedFormula: "mol-release",
		AttachedVars:    []string{"version=1.2.3", "channel=stable"},
	}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	if err := outputMoleculeStatus(status); err != nil {
		t.Fatalf("outputMoleculeStatus: %v", err)
	}

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	os.Stdout = oldStdout
	output := buf.String()

	if !strings.Contains(output, "📐 Formula: mol-release") {
		t.Fatalf("expected formula in output, got:\n%s", output)
	}
	if !strings.Contains(output, "--var version=1.2.3") || !strings.Contains(output, "--var channel=stable") {
		t.Fatalf("expected formula vars in output, got:\n%s", output)
	}
}

func TestOutputMoleculeStatus_FormulaWispShowsWorkflowContext(t *testing.T) {
	status := MoleculeStatusInfo{
		HasWork:         true,
		PinnedBead:      &beads.Issue{ID: "tool-wisp-demo", Title: "demo-hello"},
		AttachedFormula: "demo-hello",
		Progress: &MoleculeProgressInfo{
			RootID:     "tool-wisp-demo",
			RootTitle:  "demo-hello",
			TotalSteps: 3,
			DoneSteps:  0,
			ReadySteps: []string{"tool-wisp-step-1"},
		},
		NextAction: "Show the workflow steps: gt prime or bd mol current tool-wisp-demo",
	}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	if err := outputMoleculeStatus(status); err != nil {
		t.Fatalf("outputMoleculeStatus: %v", err)
	}

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	os.Stdout = oldStdout
	output := buf.String()

	if !strings.Contains(output, "📐 Formula: demo-hello") {
		t.Fatalf("expected formula line in output, got:\n%s", output)
	}
	if strings.Contains(output, "No molecule attached") {
		t.Fatalf("formula wisp should not be rendered as naked work, got:\n%s", output)
	}
	if strings.Contains(output, "Attach a molecule to start work") {
		t.Fatalf("formula wisp should not suggest gt mol attach, got:\n%s", output)
	}
	if !strings.Contains(output, "Show the workflow steps: gt prime or bd mol current tool-wisp-demo") {
		t.Fatalf("expected workflow next action, got:\n%s", output)
	}
}

// --- storePool tests ---

func TestStorePool_ResolveDirCaches(t *testing.T) {
	pool := newStorePool(context.Background())
	defer pool.close()

	dir := t.TempDir()
	beadsDir := filepath.Join(dir, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// First call resolves
	resolved1 := pool.resolveDir(dir)
	if resolved1 != beadsDir {
		t.Fatalf("expected %s, got %s", beadsDir, resolved1)
	}

	// Second call returns cached value (even if filesystem changes)
	resolved2 := pool.resolveDir(dir)
	if resolved2 != resolved1 {
		t.Fatalf("cache miss: %s != %s", resolved2, resolved1)
	}

	// Verify cache populated
	if len(pool.resolve) != 1 {
		t.Fatalf("expected 1 cached entry, got %d", len(pool.resolve))
	}
}

func TestStorePool_GetMissingDir(t *testing.T) {
	pool := newStorePool(context.Background())
	defer pool.close()

	// Non-existent directory returns nil and caches the failure
	store := pool.get("/nonexistent/path/that/does/not/exist")
	if store != nil {
		t.Fatal("expected nil for non-existent directory")
	}
	if len(pool.failed) != 1 {
		t.Fatalf("expected 1 failed entry, got %d", len(pool.failed))
	}

	// Second call hits failure cache, doesn't retry
	store = pool.get("/nonexistent/path/that/does/not/exist")
	if store != nil {
		t.Fatal("expected nil on retry")
	}
}

func TestStorePool_GetNoBeadsDir(t *testing.T) {
	pool := newStorePool(context.Background())
	defer pool.close()

	// Directory without .beads subdirectory returns nil.
	// ResolveBeadsDir still returns dir/.beads (it doesn't check existence),
	// but os.Stat catches it and the failure is cached to prevent retries.
	dir := t.TempDir()
	store := pool.get(dir)
	if store != nil {
		t.Fatal("expected nil for directory without .beads")
	}
	if len(pool.failed) != 1 {
		t.Fatalf("expected 1 failed entry (cached non-existent .beads), got %d", len(pool.failed))
	}
}

func TestStorePool_InjectNoStore(t *testing.T) {
	pool := newStorePool(context.Background())
	defer pool.close()

	b := beads.New(t.TempDir())

	// inject on a directory with no store should leave Beads unchanged
	pool.inject(b, "/nonexistent/path")
	if b.Store() != nil {
		t.Fatal("expected nil store after inject with no available store")
	}
}

func TestStorePool_CloseEmpty(t *testing.T) {
	pool := newStorePool(context.Background())
	// close on empty pool should not panic
	pool.close()
}

func TestStorePool_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	pool := newStorePool(ctx)
	defer pool.close()

	// With a cancelled context, OpenFromConfig should fail fast.
	// Create a directory with .beads and metadata.json to trigger OpenFromConfig.
	dir := t.TempDir()
	beadsDir := filepath.Join(dir, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatal(err)
	}
	// Write minimal metadata that would trigger a real open attempt
	metadata := `{"backend":"dolt","dolt_mode":"server","dolt_server_host":"127.0.0.1","dolt_server_port":39999,"dolt_database":"test"}`
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"), []byte(metadata), 0644); err != nil {
		t.Fatal(err)
	}

	// Capture stderr to verify warning message and suppress test noise.
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	store := pool.get(dir)

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	os.Stderr = oldStderr
	warning := buf.String()

	if store != nil {
		t.Fatal("expected nil store with cancelled context")
	}
	if !pool.failed[beadsDir] {
		t.Fatal("expected failure cached for cancelled context")
	}
	if !strings.Contains(warning, "store open failed") {
		t.Fatalf("expected warning on stderr, got: %q", warning)
	}
	if !strings.Contains(warning, "subprocess fallback") {
		t.Fatalf("expected fallback message in warning, got: %q", warning)
	}
}
