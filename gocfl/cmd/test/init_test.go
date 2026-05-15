package test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
)

func TestInit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gocfl_test_init")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ocflPath := filepath.Join(tempDir, "ocfl_root")

	// Da cmd.Execute() den root Befehl ausführt, können wir versuchen,
	// den init-Befehl direkt zu triggern.
	// Da viele globale Variablen in gocfl/cmd verwendet werden,
	// müssen wir sicherstellen, dass sie initialisiert sind.

	// Wir verwenden den rootCmd aus dem cmd Paket.
	cmd.ResetForTest()
	root := cmd.GetRootCmd()
	root.SetArgs([]string{"init", ocflPath})

	// Wir müssen sicherstellen, dass wir keine echten Log-Files schreiben etc.
	// Das cmd Paket scheint ocfllogger zu verwenden, der in PersistentPreRun initialisiert wird.

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() failed: %v", err)
	}

	// Liste Dateien im Verzeichnis für Debugging (optional)
	/*
		files, _ := os.ReadDir(ocflPath)
		t.Logf("Files in %s:", ocflPath)
		for _, f := range files {
			t.Logf("  %s", f.Name())
		}
	*/

	// Überprüfe ob die OCFL Struktur erstellt wurde
	// Wir verwenden das VFS für die Prüfung
	vfs := cmd.GetVFS()
	if vfs == nil {
		t.Fatal("VFS not initialized")
	}

	// In gocfl-cli wird das lokale Dateisystem normalerweise unter einem Präfix oder direkt eingebunden.
	// Da wir einen absoluten Pfad verwendet haben, sollte das VFS diesen über den lokalen FS-Treiber finden.
	// Namaste Datei prüfen
	namaste := filepath.ToSlash(filepath.Join(ocflPath, "0=ocfl_1.1"))
	if _, err := fs.Stat(vfs, namaste); err != nil {
		t.Errorf("OCFL Namaste file not found via VFS at %s: %v", namaste, err)
	}
}
