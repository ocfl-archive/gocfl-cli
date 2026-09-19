package cmd

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
)

func TestAll(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gocfl_test_init")
	require.NoError(t, err, "failed to create temp dir")
	defer os.RemoveAll(tempDir)

	ocflPath := path.Join(filepath.ToSlash(tempDir), "ocfl_root")
	_ = os.RemoveAll(ocflPath)
	_ = os.MkdirAll(ocflPath, 0755)
	defer os.RemoveAll(ocflPath)

	t.Run("init", func(t *testing.T) {
		// Since cmd.Execute() executes the root command, we can try to
		// trigger the init command directly.
		// Since many global variables are used in gocfl/cmd,
		// we must ensure they are initialized.

		// We use the rootCmd from the cmd package.
		ResetForTest()
		root := GetRootCmd()
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
	})

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

	t.Run("add", func(t *testing.T) {
		// 2. Execute add command
		ResetForTest()
		root := GetRootCmd()
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
	})

	metaOutputFile := filepath.Join(tempDir, "metadata.txt")
	t.Run("extractmeta v1", func(t *testing.T) {
		// 4. Verify metadata via extractmeta command
		ResetForTest()
		root := GetRootCmd()
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
	})

	t.Run("update", func(t *testing.T) {
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
		ResetForTest()
		root := GetRootCmd()
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
	})

	t.Run("extractmeta v2", func(t *testing.T) {
		// 9. Verify metadata after update
		metaOutputFileV2 := filepath.Join(tempDir, "metadata_v2.txt")
		ResetForTest()
		root := GetRootCmd()
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
	})

	t.Run("stat dir", func(t *testing.T) {
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"stat", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute(), "stat dir Execute() failed")

		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"stat", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-001",
		})
		require.NoError(t, root.Execute(), "stat dir object Execute() failed")
	})

	t.Run("create and stat zip", func(t *testing.T) {
		zipPath := path.Join(filepath.ToSlash(tempDir), "test_archive.zip")
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"create", zipPath, sourceDir,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-zip",
			"--message", "initial zip add",
			"--user-name", "John Doe",
			"--user-address", "john@doe.com",
		})
		require.NoError(t, root.Execute(), "create zip Execute() failed")

		// Validate zip
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", zipPath,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute(), "validate zip Execute() failed")

		// Stat zip storage root
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"stat", zipPath,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute(), "stat zip Execute() failed")

		// Stat zip object
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"stat", zipPath,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-zip",
		})
		require.NoError(t, root.Execute(), "stat zip object Execute() failed")
	})

	t.Run("validate valid dir", func(t *testing.T) {
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"validate", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute(), "validate valid dir failed")

		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-001",
		})
		require.NoError(t, root.Execute(), "validate valid object failed")
	})

	t.Run("validate conflicting flags", func(t *testing.T) {
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"validate", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--object-path", "dummy",
		})
		require.Error(t, root.Execute(), "validate with conflicting flags should fail")
	})

	t.Run("validate invalid ocfl structure returns error", func(t *testing.T) {
		invalidDir, err := os.MkdirTemp("", "gocfl_test_invalid")
		require.NoError(t, err)
		defer os.RemoveAll(invalidDir)
		invalidDir = filepath.ToSlash(invalidDir)

		// Create an OCFL root and add an object
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"init", invalidDir,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute())

		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"add", invalidDir, sourceDir,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-invalid",
			"--message", "initial add",
			"--user-name", "John Doe",
			"--user-address", "john@doe.com",
		})
		require.NoError(t, root.Execute())

		// Corrupt the object by writing invalid content to inventory.json
		found := false
		err = filepath.WalkDir(invalidDir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && d.Name() == "inventory.json" {
				found = true
				_ = os.WriteFile(p, []byte("{\"invalid\": \"json\"}"), 0644)
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, found, "inventory.json should be found and corrupted")

		// Validate the specific object - should return error
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", invalidDir,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-invalid",
		})
		require.Error(t, root.Execute(), "validate on corrupted object should return error")

		// Corrupt the storage root by removing the namaste file
		namasteFile := filepath.Join(invalidDir, "0=ocfl_1.1")
		require.NoError(t, os.Remove(namasteFile))

		// Validate the storage root - should return error
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", invalidDir,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.Error(t, root.Execute(), "validate on corrupted storage root should return error")
	})

	t.Run("validate corrupted content file returns validation failed error", func(t *testing.T) {
		corruptedDir, err := os.MkdirTemp("", "gocfl_test_corrupted_content")
		require.NoError(t, err)
		defer os.RemoveAll(corruptedDir)
		corruptedDir = filepath.ToSlash(corruptedDir)

		// Create an OCFL root and add an object
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"init", corruptedDir,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute())

		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"add", corruptedDir, sourceDir,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-tampered",
			"--message", "initial add",
			"--user-name", "John Doe",
			"--user-address", "john@doe.com",
		})
		require.NoError(t, root.Execute())

		// Tamper with a payload file inside v1/content
		tampered := false
		err = filepath.WalkDir(corruptedDir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(p, "new_file.txt") {
				tampered = true
				_ = os.WriteFile(p, []byte("tampered content that invalidates digest"), 0644)
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, tampered, "payload file new_file.txt should be found and modified")

		// Validate object specifically - should return error
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", corruptedDir,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-tampered",
		})
		execErr := root.Execute()
		require.Error(t, execErr, "validate on tampered object should fail")
		require.Contains(t, execErr.Error(), "validation failed")
	})

	t.Run("validate config preservation", func(t *testing.T) {
		ResetForTest()
		// Simulate pre-configured ObjectID from config
		conf.Validate.ObjectID = "test-obj-001"
		root := GetRootCmd()
		root.SetArgs([]string{
			"validate", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
		})
		require.NoError(t, root.Execute(), "validate should preserve preset ObjectID from config")

		// Verify that CLI flag overrides preset ObjectID in config
		ResetForTest()
		conf.Validate.ObjectID = "non-existent-obj-id"
		root = GetRootCmd()
		root.SetArgs([]string{
			"validate", ocflPath,
			"--log-level", "DEBUG",
			"--config", "internal",
			"--object-id", "test-obj-001",
		})
		require.NoError(t, root.Execute(), "validate flag should override preset ObjectID from config")
	})

	t.Run("add error cases", func(t *testing.T) {
		// Missing required flags
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"add", ocflPath, sourceDir,
			"--config", "internal",
		})
		require.Error(t, root.Execute(), "add without required flags should fail")

		// Non-existent source dir
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"add", ocflPath, "C:/non_existent_dir_xyz_123",
			"--config", "internal",
			"--object-id", "test-obj-new",
			"--message", "add msg",
			"--user-name", "John",
			"--user-address", "john@test.com",
		})
		require.Error(t, root.Execute(), "add with non-existent source dir should fail")

		// Adding already existing object
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"add", ocflPath, sourceDir,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--message", "add msg",
			"--user-name", "John",
			"--user-address", "john@test.com",
		})
		require.Error(t, root.Execute(), "adding already existing object id should fail")
	})

	t.Run("update error cases", func(t *testing.T) {
		// Updating non-existent object
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"update", ocflPath, sourceDir,
			"--config", "internal",
			"--object-id", "non-existent-obj-id-999",
			"--message", "update msg",
			"--user-name", "John",
			"--user-address", "john@test.com",
		})
		require.Error(t, root.Execute(), "updating non-existent object should fail")

		// Invalid digest
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"update", ocflPath, sourceDir,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--message", "update msg",
			"--user-name", "John",
			"--user-address", "john@test.com",
			"--digest", "invalid_digest_alg",
		})
		require.Error(t, root.Execute(), "update with invalid digest should fail")
	})

	t.Run("stat error cases", func(t *testing.T) {
		// Conflicting flags
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"stat", ocflPath,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--object-path", "dummy-path",
		})
		require.Error(t, root.Execute(), "stat with conflicting flags should fail")

		// Invalid stat-info
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"stat", ocflPath,
			"--config", "internal",
			"--stat-info", "invalid_stat_field",
		})
		require.Error(t, root.Execute(), "stat with invalid stat-info should fail")
	})

	t.Run("extract error cases", func(t *testing.T) {
		// Conflicting flags
		targetDir, err := os.MkdirTemp("", "gocfl_test_extract_err")
		require.NoError(t, err)
		defer os.RemoveAll(targetDir)
		targetDir = filepath.ToSlash(targetDir)

		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"extract", ocflPath, targetDir,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--object-path", "dummy-path",
		})
		require.Error(t, root.Execute(), "extract with conflicting flags should fail")

		// Non-empty target folder
		dummyFile := filepath.Join(targetDir, "existing.txt")
		require.NoError(t, os.WriteFile(dummyFile, []byte("existing"), 0644))

		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"extract", ocflPath, targetDir,
			"--config", "internal",
			"--object-id", "test-obj-001",
		})
		require.Error(t, root.Execute(), "extract into non-empty target dir should fail")
	})

	t.Run("extractmeta error cases", func(t *testing.T) {
		// Conflicting flags
		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"extractmeta", ocflPath,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--object-path", "dummy-path",
		})
		require.Error(t, root.Execute(), "extractmeta with conflicting flags should fail")

		// Missing both object-id and object-path
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"extractmeta", ocflPath,
			"--config", "internal",
		})
		require.Error(t, root.Execute(), "extractmeta without object-id or object-path should fail")

		// Invalid format
		ResetForTest()
		root = GetRootCmd()
		root.SetArgs([]string{
			"extractmeta", ocflPath,
			"--config", "internal",
			"--object-id", "test-obj-001",
			"--format", "xml_invalid",
		})
		require.Error(t, root.Execute(), "extractmeta with invalid format should fail")
	})

	t.Run("init error cases", func(t *testing.T) {
		dummyInitDir, err := os.MkdirTemp("", "gocfl_test_init_err")
		require.NoError(t, err)
		defer os.RemoveAll(dummyInitDir)
		dummyInitDir = filepath.ToSlash(dummyInitDir)

		ResetForTest()
		root := GetRootCmd()
		root.SetArgs([]string{
			"init", dummyInitDir,
			"--config", "internal",
			"--digest", "invalid_digest_name",
		})
		require.Error(t, root.Execute(), "init with invalid digest should fail")
	})
}
