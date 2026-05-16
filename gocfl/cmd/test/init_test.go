package test

import (
	"image"
	"image/color"
	"image/draw"
	"image/png"
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

	// Since cmd.Execute() executes the root command, we can try to
	// trigger the init command directly.
	// Since many global variables are used in gocfl/cmd,
	// we must ensure they are initialized.

	// We use the rootCmd from the cmd package.
	cmd.ResetForTest()
	root := cmd.GetRootCmd()
	root.SetArgs([]string{
		"init", ocflPath,
		"--log-level", "DEBUG",
		"--config", "internal",
	})

	// We need to ensure that we don't write real log files, etc.
	// The cmd package seems to use ocfllogger, which is initialized in PersistentPreRun.

	require.NoError(t, root.Execute(), "Execute() failed")

	// In gocfl-cli, the local filesystem is usually mounted under a prefix or directly.
	// Since we used an absolute path, the VFS should find it via the local FS driver.
	// Check Namaste file
	namaste := path.Join(ocflPath, "0=ocfl_1.1")
	_, err = os.Stat(namaste)
	require.NoErrorf(t, err, "OCFL Namaste file not found at %s", namaste)

	// Add OCFL object
	// 1. Create test data
	sourceDir, err := os.MkdirTemp("", "gocfl_test_source")
	require.NoError(t, err, "failed to create source dir")
	sourceDir = filepath.ToSlash(sourceDir)
	defer os.RemoveAll(sourceDir)

	testFiles := []string{
		"file1.txt",
		"file2.txt",
		"sub 1/file3.txt",
		"sub2/subsub[1]/file4.txt",
		"image.png",
	}
	for _, f := range testFiles {
		fullPath := filepath.Join(sourceDir, f)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		if strings.HasSuffix(f, ".png") {
			img := image.NewRGBA(image.Rect(0, 0, 640, 480))
			// Fill with a simple color (e.g., light blue)
			draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{R: 173, G: 216, B: 230, A: 255}}, image.Point{}, draw.Src)
			f, err := os.Create(fullPath)
			require.NoError(t, err)
			err = png.Encode(f, img)
			require.NoError(t, err)
			err = f.Close()
			require.NoError(t, err)
		} else {
			err = os.WriteFile(fullPath, []byte("content of "+f), 0644)
			require.NoError(t, err)
		}
	}

	// 2. Execute add command
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

	// 3. Check if the object was created
	// In OCFL 1.1, the object is usually located in a subdirectory
	// determined by the storage layout. Since we didn't specify anything special
	// during init, a default layout (e.g., flat or hashed) is likely used.
	// We check for the existence of the object by searching for its inventory.json.
	found := false
	err = filepath.WalkDir(ocflPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		t.Logf("  %s", strings.TrimPrefix(filepath.ToSlash(path), ocflPath))
		if !d.IsDir() && d.Name() == "inventory.json" {
			// Check if the ID is present in inventory.json
			content, err := os.ReadFile(path)
			if err == nil && strings.Contains(string(content), "test-obj-001") {
				found = true
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "Object 'test-obj-001' not found in OCFL root")

	// 4. Verify metadata via extractmeta command
	metaOutputFile := filepath.Join(tempDir, "metadata.txt")
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	root.SetArgs([]string{
		"extractmeta", ocflPath,
		"--log-level", "DEBUG",
		"--config", "internal",
		"--object-id", "test-obj-001",
		"--output", metaOutputFile,
		"--format", "human",
	})
	require.NoError(t, root.Execute(), "extractmeta Execute() failed")

	// 5. Read and verify metadata file
	metaDataBytes, err := os.ReadFile(metaOutputFile)
	require.NoError(t, err, "failed to read metadata output file")

	metaDataStr := string(metaDataBytes)
	require.Contains(t, metaDataStr, "Object ID: test-obj-001", "Object ID mismatch in metadata")
	require.Contains(t, metaDataStr, "image.png", "image.png not found in object metadata")

	t.Log("Successfully verified metadata via extractmeta command")

	// 6. Output human-readable metadata
	//	t.Logf("Metadata content:\n%s", metaDataStr)

	// 7. Update test: renames, deletions, duplicates, and new files
	// Delete file: file1.txt
	err = os.Remove(filepath.Join(sourceDir, "file1.txt"))
	require.NoError(t, err)

	// Rename file: file2.txt -> file2_renamed.txt
	err = os.Rename(filepath.Join(sourceDir, "file2.txt"), filepath.Join(sourceDir, "file2_renamed.txt"))
	require.NoError(t, err)

	// Add new file: new_file.txt
	err = os.WriteFile(filepath.Join(sourceDir, "new_file.txt"), []byte("content of new_file.txt"), 0644)
	require.NoError(t, err)

	// Create duplicate: duplicate.png (copy content of image.png)
	imgContent, err := os.ReadFile(filepath.Join(sourceDir, "image.png"))
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(sourceDir, "duplicate.png"), imgContent, 0644)
	require.NoError(t, err)

	// 8. Execute update command
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	root.SetArgs([]string{
		"update", ocflPath, sourceDir,
		"--log-level", "DEBUG",
		"--config", "internal",
		"--object-id", "test-obj-001",
		"--message", "update with deletions, renames and duplicates",
		"--user-name", "Jane Doe",
		"--user-address", "jane@doe.com",
	})
	require.NoError(t, root.Execute(), "update Execute() failed")

	// 9. Verify metadata after update
	metaOutputFileV2 := filepath.Join(tempDir, "metadata_v2.txt")
	cmd.ResetForTest()
	root = cmd.GetRootCmd()
	root.SetArgs([]string{
		"extractmeta", ocflPath,
		"--log-level", "DEBUG",
		"--config", "internal",
		"--object-id", "test-obj-001",
		"--output", metaOutputFileV2,
		"--format", "human",
	})
	require.NoError(t, root.Execute(), "extractmeta V2 Execute() failed")

	metaDataBytesV2, err := os.ReadFile(metaOutputFileV2)
	require.NoError(t, err)
	metaDataStrV2 := string(metaDataBytesV2)

	require.Contains(t, metaDataStrV2, "Head: v2", "Head v2 not found in metadata")
	require.Contains(t, metaDataStrV2, "new_file.txt", "new_file.txt not found in v2")
	require.Contains(t, metaDataStrV2, "file2_renamed.txt", "file2_renamed.txt not found in v2")
	require.Contains(t, metaDataStrV2, "duplicate.png", "duplicate.png not found in v2")
	// "file1.txt" should only appear in history (v1), but not as an active filename in v2.
	// The String() method of Metadata lists all files and their versions.
	// We check if "Version v2" is NOT associated with "file1.txt".
	require.NotRegexp(t, `file1\.txt\s+Version.*v2`, metaDataStrV2, "file1.txt should not be present in v2")

	t.Logf("Updated Metadata content (v2):\n%s", metaDataStrV2)
}
