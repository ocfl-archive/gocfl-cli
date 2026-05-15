package test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gocfl_test_add")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	currentDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(currentDir)

	// 1. Initialisiere OCFL Struktur
	cmd.ResetForTest()
	root := cmd.GetRootCmd()
	ocflRoot := "ocfl_root"
	root.SetArgs([]string{"init", ocflRoot})
	err = root.Execute()
	require.NoError(t, err)

	// 2. Erstelle Quelldaten zum Hinzufügen
	srcDir := "source_data"
	err = os.Mkdir(srcDir, 0755)
	require.NoError(t, err)

	testFile := "test.txt"
	testContent := "hello ocfl"
	err = os.WriteFile(filepath.Join(srcDir, testFile), []byte(testContent), 0644)
	require.NoError(t, err)

	// 3. Füge Objekt hinzu
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	objID := "urn:uuid:12345678-1234-1234-1234-1234567890ab"

	root.SetArgs([]string{"add", ocflRoot, srcDir, "--object-id", objID, "-m", "initial add", "-u", "Junie", "-a", "mailto:junie@jetbrains.com"})

	err = root.Execute()
	require.NoError(t, err)

	// 4. Verifiziere das Ergebnis via VFS
	vfs := cmd.GetVFS()
	require.NotNil(t, vfs)
	vfsRoot := vfs.RealPath(ocflRoot)

	// Prüfe ob Storage Root Namaste existiert via VFS
	rootNamaste := path.Join(vfsRoot, "0=ocfl_1.1")
	_, err = fs.Stat(vfs, rootNamaste)
	require.NoError(t, err)

	// Prüfe ob das Objekt im Storage Root existiert
	found := false
	err = fs.WalkDir(vfs, vfsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Base(path) == "0=ocfl_object_1.1" {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)

	// Prüfe spezifisch auf die Testdatei im VFS
	fileFound := false
	err = fs.WalkDir(vfs, vfsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Base(path) == testFile {
			fileFound = true
			return fs.SkipAll
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, fileFound)
}
