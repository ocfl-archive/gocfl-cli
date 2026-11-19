package tests

import (
	"archive/zip"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleROCrateMetadataJSON contains a standard RO-Crate (v1.1) metadata descriptor
// defining a Dataset root entity ("./") with title, description, keywords, license,
// and datePublished.
const sampleROCrateMetadataJSON = `{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@type": "CreativeWork",
      "@id": "ro-crate-metadata.json",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": [
        "Dataset",
        "LearningResource"
      ],
      "name": "Research Dataset Crate",
      "description": "A comprehensive RO-Crate package for dataset archival testing",
      "datePublished": "2024-03-08",
      "license": {
        "@id": "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html"
      },
      "keywords": [
        "research",
        "galaxy",
        "training"
      ]
    }
  ]
}`

// sampleROCrateMetadataSecondObjectJSON contains a second distinct RO-Crate descriptor
// used to test independent multi-object ingestion with unique metadata attributes.
const sampleROCrateMetadataSecondObjectJSON = `{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@type": "CreativeWork",
      "@id": "ro-crate-metadata.json",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": [
        "Dataset",
        "SoftwareSourceCode"
      ],
      "name": "Bioinformatics Workflow Crate",
      "description": "Genomic sequence alignment workflow and reference annotations",
      "datePublished": "2024-06-15",
      "license": {
        "@id": "https://spdx.org/licenses/Apache-2.0"
      },
      "keywords": [
        "bioinformatics",
        "workflow",
        "genomics"
      ]
    }
  ]
}`

// setupROCrateSourceDir prepares a temporary directory with RO-Crate metadata and payload files.
func setupROCrateSourceDir(t *testing.T, baseDir string, folderName string, metadataContent string) string {
	t.Helper()
	srcDir := filepath.Join(baseDir, folderName)
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "data"), 0755))

	// Write ro-crate-metadata.json in the root of the source directory
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "ro-crate-metadata.json"), []byte(metadataContent), 0644))

	// Write payload data files
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data", "experiment.csv"), []byte("sample_id,value\n1,100\n2,200\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data", "notes.txt"), []byte("Experiment observation notes"), 0644))

	return srcDir
}

// TestROCrateIntegration tests the complete integration of the RO-Crate OCFL extension (NNNN-ro-crate).
//
// The RO-Crate extension parses `ro-crate-metadata.json` present in ingested OCFL objects,
// extracts core descriptive metadata fields (such as title, description, date, keywords, license,
// organization, and user metadata), and writes a summarized descriptor (`info.json`) into the
// object's extension directory (`extensions/NNNN-ro-crate/info.json`).
//
// This test suite covers:
//  1. Creation and Ingestion (`gocfl create`): Verify storage root creation, object generation,
//     RO-Crate extension metadata mapping, and `info.json` extraction.
//  2. Multi-Object Lifecycle (`gocfl init` -> `gocfl add`): Verify storage root initialization
//     and consecutive ingestion of distinct RO-Crate objects with separate metadata.
//  3. Metadata Extraction (`gocfl extractmeta`): Export metadata to JSON and inspect output structure.
//  4. OCFL Storage Validation (`gocfl validate`) and Object Status (`gocfl stat`).
//  5. Direct ZIP Container Creation: Verify compressed ZIP storage root generation with RO-Crate extension.
//  6. Disabled Extension Verification: Ensure no metadata mapping occurs when the extension is disabled.
func TestROCrateIntegration(t *testing.T) {
	// =========================================================================
	// Scenario 1: Object Creation with RO-Crate Metadata on Folder Storage Root
	// =========================================================================
	t.Run("CreateAndExtractMeta_FolderRoot", func(t *testing.T) {
		tempDir := t.TempDir()
		srcDir := setupROCrateSourceDir(t, tempDir, "src_rocrate_01", sampleROCrateMetadataJSON)
		ocflDir := filepath.Join(tempDir, "ocfl_storage")
		objectID := "urn:uuid:test-rocrate-object-01"

		t.Log("[Step 1] Execute 'gocfl create' to create OCFL storage root and ingest RO-Crate object...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"create",
			ocflDir,
			srcDir,
			"--config", "internal",
			"--object-id", objectID,
			"-m", "Initial ingest with RO-Crate metadata",
			"-u", "Dr. Jane Doe",
			"-a", "mailto:jane.doe@example.org",
			"--ext-NNNN-ro-crate-enabled", "true",
			"--ext-NNNN-ro-crate-organisation", "Open Science Institute",
			"--ext-NNNN-ro-crate-organisationID", "https://ror.org/04z8jg394",
			"--ext-NNNN-ro-crate-user", "Dr. Jane Doe",
			"--ext-NNNN-ro-crate-address", "Science Park 101, 8000 Zurich",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl create must succeed")

		t.Log("[Step 2] Verify object structure and RO-Crate extension info.json...")
		var extensionConfigPath, extensionInfoPath string
		err := filepath.WalkDir(ocflDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				if strings.HasSuffix(filepath.ToSlash(path), "extensions/NNNN-ro-crate/config.json") {
					extensionConfigPath = path
				}
				if strings.HasSuffix(filepath.ToSlash(path), "extensions/NNNN-ro-crate/info.json") {
					extensionInfoPath = path
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.NotEmpty(t, extensionConfigPath, "extensions/NNNN-ro-crate/config.json should be present in object")
		require.NotEmpty(t, extensionInfoPath, "extensions/NNNN-ro-crate/info.json should be present in object")

		// Verify extension configuration file contents
		configData, err := os.ReadFile(extensionConfigPath)
		require.NoError(t, err)
		var configMap map[string]any
		require.NoError(t, json.Unmarshal(configData, &configMap))
		assert.Equal(t, "NNNN-ro-crate", configMap["extensionName"])

		// Verify parsed RO-Crate metadata extracted to info.json
		infoData, err := os.ReadFile(extensionInfoPath)
		require.NoError(t, err)
		var infoMap map[string]any
		require.NoError(t, json.Unmarshal(infoData, &infoMap))

		assert.Equal(t, "./", infoMap["signature"], "signature should match dataset root './'")
		assert.Equal(t, "Research Dataset Crate", infoMap["title"])
		assert.Equal(t, "A comprehensive RO-Crate package for dataset archival testing", infoMap["description"])
		assert.Equal(t, "2024-03-08", infoMap["created"])
		assert.Equal(t, "Dataset; LearningResource", infoMap["sets"])
		assert.Equal(t, "research; galaxy; training", infoMap["keywords"])
		assert.Equal(t, "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html", infoMap["licenses"])
		assert.Equal(t, "Open Science Institute", infoMap["organisation"])
		assert.Equal(t, "https://ror.org/04z8jg394", infoMap["organisation_id"])
		assert.Equal(t, "Dr. Jane Doe", infoMap["user"])
		assert.Equal(t, "Science Park 101, 8000 Zurich", infoMap["address"])

		t.Log("[Step 3] Execute 'gocfl extractmeta' and verify JSON metadata output...")
		metaOutputFile := filepath.Join(tempDir, "extracted_meta.json")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"extractmeta",
			ocflDir,
			"--config", "internal",
			"--object-id", objectID,
			"--format", "json",
			"--output", metaOutputFile,
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl extractmeta must succeed")
		require.FileExists(t, metaOutputFile)

		extractedBytes, err := os.ReadFile(metaOutputFile)
		require.NoError(t, err)
		var metaOutput map[string]any
		require.NoError(t, json.Unmarshal(extractedBytes, &metaOutput))
		require.NotEmpty(t, metaOutput, "extracted metadata should not be empty")

		t.Log("[Step 4] Execute 'gocfl validate' on storage root...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"validate",
			ocflDir,
			"--config", "internal",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl validate on storage root must pass")

		t.Log("[Step 5] Execute 'gocfl stat' on storage root and object...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"stat",
			ocflDir,
			"--config", "internal",
			"--object-id", objectID,
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl stat must succeed")
	})

	// =========================================================================
	// Scenario 2: Multi-Object Ingestion Lifecycle (init -> add Object 1 -> add Object 2)
	// =========================================================================
	t.Run("Lifecycle_InitAndAddMultipleObjects", func(t *testing.T) {
		tempDir := t.TempDir()
		srcDir1 := setupROCrateSourceDir(t, tempDir, "src_dataset_1", sampleROCrateMetadataJSON)
		srcDir2 := setupROCrateSourceDir(t, tempDir, "src_dataset_2", sampleROCrateMetadataSecondObjectJSON)
		ocflDir := filepath.Join(tempDir, "ocfl_multi_obj")

		objectID1 := "urn:uuid:test-rocrate-dataset-01"
		objectID2 := "urn:uuid:test-rocrate-dataset-02"

		t.Log("[Step 1] Execute 'gocfl init' to initialize empty OCFL storage root...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"init",
			ocflDir,
			"--config", "internal",
			"--ocfl-version", "1.1",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl init must succeed")

		t.Log("[Step 2] Ingest Object 1 with RO-Crate metadata via 'gocfl add'...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"add",
			ocflDir,
			srcDir1,
			"--config", "internal",
			"--object-id", objectID1,
			"-m", "Ingesting first RO-Crate dataset",
			"-u", "Lead Researcher",
			"-a", "mailto:lead@example.org",
			"--ext-NNNN-ro-crate-enabled", "true",
			"--ext-NNNN-ro-crate-organisation", "Swiss Data Center",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl add for object 1 must succeed")

		t.Log("[Step 3] Ingest Object 2 with distinct RO-Crate metadata via 'gocfl add'...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"add",
			ocflDir,
			srcDir2,
			"--config", "internal",
			"--object-id", objectID2,
			"-m", "Ingesting second RO-Crate dataset",
			"-u", "Bioinformatics Team",
			"-a", "mailto:bio@example.org",
			"--ext-NNNN-ro-crate-enabled", "true",
			"--ext-NNNN-ro-crate-organisation", "Genomics Laboratory",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl add for object 2 must succeed")

		t.Log("[Step 4] Extract and verify metadata for both objects...")
		metaOutputFile1 := filepath.Join(tempDir, "meta_obj1.json")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"extractmeta",
			ocflDir,
			"--config", "internal",
			"--object-id", objectID1,
			"--format", "json",
			"--output", metaOutputFile1,
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "extractmeta for object 1 must succeed")

		metaOutputFile2 := filepath.Join(tempDir, "meta_obj2.json")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"extractmeta",
			ocflDir,
			"--config", "internal",
			"--object-id", objectID2,
			"--format", "json",
			"--output", metaOutputFile2,
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "extractmeta for object 2 must succeed")

		t.Log("[Step 5] Validate multi-object storage root...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"validate",
			ocflDir,
			"--config", "internal",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl validate on multi-object root must pass")
	})

	// =========================================================================
	// Scenario 3: Creation and Verification in ZIP Archive Storage
	// =========================================================================
	t.Run("CreateInZipContainer", func(t *testing.T) {
		tempDir := t.TempDir()
		srcDir := setupROCrateSourceDir(t, tempDir, "src_rocrate_zip", sampleROCrateMetadataJSON)
		zipPath := filepath.Join(tempDir, "archive.zip")
		objectID := "urn:uuid:test-rocrate-zip"

		t.Log("[Step 1] Execute 'gocfl create' targeting a ZIP archive with RO-Crate extension...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"create",
			zipPath,
			srcDir,
			"--config", "internal",
			"--object-id", objectID,
			"-m", "Ingesting RO-Crate into ZIP archive",
			"-u", "Zip Packager",
			"-a", "mailto:packager@example.org",
			"--ext-NNNN-ro-crate-enabled", "true",
			"--ext-NNNN-ro-crate-organisation", "Zip Archive Foundation",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl create in zip must succeed")
		require.FileExists(t, zipPath)

		t.Log("[Step 2] Verify ZIP contents contain RO-Crate extension metadata file...")
		zr, err := zip.OpenReader(zipPath)
		require.NoError(t, err)
		defer zr.Close()

		var foundExtensionConfig, foundExtensionInfo bool
		for _, f := range zr.File {
			if strings.HasSuffix(f.Name, "extensions/NNNN-ro-crate/config.json") {
				foundExtensionConfig = true
			}
			if strings.HasSuffix(f.Name, "extensions/NNNN-ro-crate/info.json") {
				foundExtensionInfo = true
				rc, err := f.Open()
				require.NoError(t, err)
				var info map[string]any
				require.NoError(t, json.NewDecoder(rc).Decode(&info))
				_ = rc.Close()

				assert.Equal(t, "Research Dataset Crate", info["title"])
				assert.Equal(t, "Zip Archive Foundation", info["organisation"])
			}
		}
		assert.True(t, foundExtensionConfig, "config.json must exist in ZIP archive")
		assert.True(t, foundExtensionInfo, "info.json must exist in ZIP archive")

		t.Log("[Step 3] Execute 'gocfl extractmeta' directly from ZIP archive...")
		metaOutputFile := filepath.Join(tempDir, "zip_extracted_meta.json")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"extractmeta",
			zipPath,
			"--config", "internal",
			"--object-id", objectID,
			"--format", "json",
			"--output", metaOutputFile,
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl extractmeta on ZIP must succeed")

		t.Log("[Step 4] Validate ZIP archive...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"validate",
			zipPath,
			"--config", "internal",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl validate on ZIP archive must pass")
	})

	// =========================================================================
	// Scenario 4: Ingestion with RO-Crate Extension Disabled
	// =========================================================================
	t.Run("DisabledROCrateExtension", func(t *testing.T) {
		tempDir := t.TempDir()
		srcDir := setupROCrateSourceDir(t, tempDir, "src_disabled", sampleROCrateMetadataJSON)
		ocflDir := filepath.Join(tempDir, "ocfl_disabled")
		objectID := "urn:uuid:test-disabled-rocrate"

		t.Log("[Step 1] Execute 'gocfl create' with RO-Crate disabled (default)...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"create",
			ocflDir,
			srcDir,
			"--config", "internal",
			"--object-id", objectID,
			"-m", "Ingesting with RO-Crate disabled",
			"-u", "Default User",
			"-a", "mailto:user@example.org",
			"--ext-NNNN-ro-crate-enabled", "false",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl create must succeed")

		t.Log("[Step 2] Verify that RO-Crate info.json was NOT generated...")
		var foundInfoJSON bool
		err := filepath.WalkDir(ocflDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(filepath.ToSlash(path), "extensions/NNNN-ro-crate/info.json") {
				foundInfoJSON = true
			}
			return nil
		})
		require.NoError(t, err)
		assert.False(t, foundInfoJSON, "info.json must not be created when RO-Crate is disabled")

		t.Log("[Step 3] Validate storage root...")
		cmd.ResetForTest()
		rootCmd = cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"validate",
			ocflDir,
			"--config", "internal",
			"--log-level", "DEBUG",
		})
		require.NoError(t, rootCmd.Execute(), "gocfl validate must succeed")
	})
}
