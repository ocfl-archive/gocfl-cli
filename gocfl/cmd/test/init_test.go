package test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
)

func TestInit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gocfl_test_init")
	require.NoError(t, err, "failed to create temp dir")
	defer os.RemoveAll(tempDir)

	ocflPath := path.Join(filepath.ToSlash(tempDir), "ocfl_root")
	_ = os.RemoveAll(ocflPath)
	_ = os.MkdirAll(ocflPath, 0755)
	defer os.RemoveAll(ocflPath)

	// Da cmd.Execute() den root Befehl ausführt, können wir versuchen,
	// den init-Befehl direkt zu triggern.
	// Da viele globale Variablen in gocfl/cmd verwendet werden,
	// müssen wir sicherstellen, dass sie initialisiert sind.

	// Wir verwenden den rootCmd aus dem cmd Paket.
	cmd.ResetForTest()
	root := cmd.GetRootCmd()
	root.SetArgs([]string{
		"init", ocflPath,
		"--log-level", "DEBUG",
		"--config", "internal",
	})

	// Wir müssen sicherstellen, dass wir keine echten Log-Files schreiben etc.
	// Das cmd Paket scheint ocfllogger zu verwenden, der in PersistentPreRun initialisiert wird.

	require.NoError(t, root.Execute(), "Execute() failed")

	// In gocfl-cli wird das lokale Dateisystem normalerweise unter einem Präfix oder direkt eingebunden.
	// Da wir einen absoluten Pfad verwendet haben, sollte das VFS diesen über den lokalen FS-Treiber finden.
	// Namaste Datei prüfen
	namaste := path.Join(ocflPath, "0=ocfl_1.1")
	_, err = os.Stat(namaste)
	require.NoErrorf(t, err, "OCFL Namaste file not found at %s", namaste)

	// OCFL-Objekt hinzufügen
	// 1. Testdaten erstellen
	sourceDir, err := os.MkdirTemp("", "gocfl_test_source")
	require.NoError(t, err, "failed to create source dir")
	sourceDir = filepath.ToSlash(sourceDir)
	defer os.RemoveAll(sourceDir)

	testFiles := []string{
		"file1.txt",
		"file2.txt",
		"sub1/file3.txt",
		"sub2/subsub1/file4.txt",
	}
	for _, f := range testFiles {
		fullPath := filepath.Join(sourceDir, f)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte("content of "+f), 0644)
		require.NoError(t, err)
	}

	// 2. add-Befehl ausführen
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	root.SetArgs([]string{
		"add", ocflPath, sourceDir,
		"--log-level", "DEBUG",
		"--config", "internal",
		"--object-id", "test-obj-001",
		"--message", "initial add",
		"--user-name", "John Doe",
		"--user-address", "john@doe.com",
	})
	require.NoError(t, root.Execute(), "add Execute() failed")

	// 3. Überprüfen, ob das Objekt angelegt wurde
	// Bei OCFL 1.1 liegt das Objekt standardmäßig in einem Unterverzeichnis,
	// das vom Storage Layout bestimmt wird. Da wir bei init nichts spezielles angegeben haben,
	// wird vermutlich ein Standard-Layout verwendet (z.B. flat oder hashed).
	// Wir prüfen einfach, ob ein Verzeichnis für die ID existiert, wenn wir den Pfad kennen würden.
	// Da wir das Layout nicht fest vorgegeben haben, suchen wir nach der inventory.json des Objekts.
	found := false
	err = filepath.WalkDir("C:/Users/micro/AppData/Local/Temp/gocfl_test_init_fixed/ocfl_root", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "inventory.json" {
			// Wir schauen ob im inventory.json die ID steht
			content, err := os.ReadFile(path)
			if err == nil && strings.Contains(string(content), "test-obj-001") {
				found = true
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "Object 'test-obj-001' not found in OCFL root")
}
