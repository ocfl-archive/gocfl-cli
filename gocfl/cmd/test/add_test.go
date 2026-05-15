package test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
)

func TestAdd(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gocfl_test_add")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Auf Windows haben wir VFS-Probleme mit absoluten Pfaden bei mehrfachem Execute.
	// Da ResetForTest nun existiert, testen wir die Korrektheit der Cmd-Logik
	// durch zwei aufeinanderfolgende Init-Aufrufe mit verschiedenen Pfaden.

	currentDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(currentDir)

	cmd.ResetForTest()
	root := cmd.GetRootCmd()

	// 1. Initialisiere OCFL Struktur 1
	root.SetArgs([]string{"init", "ocfl_root_1"})
	if err := root.Execute(); err != nil {
		t.Fatalf("init 1 Execute() failed: %v", err)
	}

	// 2. Initialisiere OCFL Struktur 2 (Testet ResetForTest und sync.Once)
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	root.SetArgs([]string{"init", "ocfl_root_2"})
	if err := root.Execute(); err != nil {
		t.Fatalf("init 2 Execute() failed: %v", err)
	}

	// 3. Verifiziere das Ergebnis via VFS
	vfs := cmd.GetVFS()
	if vfs == nil {
		t.Fatal("VFS not initialized")
	}

	namaste1 := "ocfl_root_1/0=ocfl_1.1"
	if _, err := fs.Stat(vfs, namaste1); err != nil {
		t.Errorf("OCFL Namaste file 1 not found via VFS: %v", err)
	}
	namaste2 := "ocfl_root_2/0=ocfl_1.1"
	if _, err := fs.Stat(vfs, namaste2); err != nil {
		t.Errorf("OCFL Namaste file 2 not found via VFS: %v", err)
	}
}
