package tests

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	statickms "github.com/je4/utils/v2/pkg/StaticKMS"
	"github.com/je4/utils/v2/pkg/encrypt"
	"github.com/je4/utils/v2/pkg/keepass2kms"
	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/streamingaead"
	"github.com/tobischo/gokeepasslib/v3"
)

func createTestKeePassDB(t *testing.T, dbPath, dbPassword, entryTitle, entryPassword string) {
	file, err := os.Create(dbPath)
	require.NoError(t, err)
	defer file.Close()

	rootGroup := gokeepasslib.NewGroup()
	rootGroup.Name = "Root"

	entry := gokeepasslib.NewEntry()
	entry.Values = append(entry.Values, gokeepasslib.ValueData{
		Key:   "Title",
		Value: gokeepasslib.V{Content: entryTitle},
	})
	entry.Values = append(entry.Values, gokeepasslib.ValueData{
		Key:   "Password",
		Value: gokeepasslib.V{Content: entryPassword},
	})

	rootGroup.Entries = append(rootGroup.Entries, entry)

	db := gokeepasslib.NewDatabase()
	db.Credentials = gokeepasslib.NewPasswordCredentials(dbPassword)
	db.Content = &gokeepasslib.DBContent{
		Meta: gokeepasslib.NewMetaData(),
		Root: &gokeepasslib.RootData{
			Groups: []gokeepasslib.Group{rootGroup},
		},
	}

	require.NoError(t, db.LockProtectedEntries())
	encoder := gokeepasslib.NewEncoder(file)
	require.NoError(t, encoder.Encode(db))
}

func TestEncryptionLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	contentDir := filepath.Join(tempDir, "content")
	require.NoError(t, os.MkdirAll(filepath.Join(contentDir, "subdir"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(contentDir, "doc.txt"), []byte("Confidential documentation data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(contentDir, "subdir", "nested.bin"), []byte{0xDE, 0xAD, 0xBE, 0xEF}, 0644))
	t.Logf("[Setup] Testdatenverzeichnis vorbereitet: %s", contentDir)

	t.Run("Static AES KMS Integration", func(t *testing.T) {
		zipPath := filepath.Join(tempDir, "static_archive.zip")
		aesKey := "01234567890123456789012345678901" // 32 bytes

		t.Log("[Static AES] 1. Erstelle verschlüsselten OCFL-Container mit statischem AES-Schlüssel via gocfl create...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"create",
			zipPath,
			contentDir,
			"--object-id", "id:test-static-aes",
			"-m", "encrypted archive creation",
			"-u", "Crypto Admin",
			"-a", "mailto:crypto@example.com",
			"--encrypt-aes",
			"--aes-key", aesKey,
			"--log-level", "DEBUG",
		})

		require.NoError(t, rootCmd.Execute())

		// Verify files created
		aesFile := zipPath + ".aes"
		keyFile := zipPath + ".aes.key.json"
		t.Logf("[Static AES] 2. Verifiziere erzeugte Artefakte (.zip, .aes, .aes.key.json): %s", zipPath)
		require.FileExists(t, zipPath)
		require.FileExists(t, aesFile)
		require.FileExists(t, keyFile)

		// Decrypt archive using StaticKMS client
		t.Log("[Static AES] 3. Lese Key-Struktur und initialisiere StaticKMS-Client...")
		keyData, err := os.ReadFile(keyFile)
		require.NoError(t, err)

		kStruct := &encrypt.KeyStruct{}
		require.NoError(t, json.Unmarshal(keyData, kStruct))

		client, err := statickms.NewClient(aesKey)
		require.NoError(t, err)

		aead, err := client.GetAEAD("static://")
		require.NoError(t, err)

		kh, err := keyset.Read(keyset.NewBinaryReader(bytes.NewBuffer(kStruct.EncryptedKey)), aead)
		require.NoError(t, err)

		stream, err := streamingaead.New(kh)
		require.NoError(t, err)

		fp, err := os.Open(aesFile)
		require.NoError(t, err)
		defer fp.Close()

		t.Log("[Static AES] 4. Entschlüssele Datenstrom mittels StreamingAEAD...")
		decReader, err := stream.NewDecryptingReader(fp, kStruct.Aad)
		require.NoError(t, err)

		decryptedBytes, err := io.ReadAll(decReader)
		require.NoError(t, err)
		t.Logf("[Static AES] -> %d Bytes erfolgreich aus .aes-Container entschlüsselt", len(decryptedBytes))

		// Verify that decrypted .aes is byte-for-byte identical and has the same checksum as the plain .zip file
		plainZipData, err := os.ReadFile(zipPath)
		require.NoError(t, err)
		require.Equal(t, plainZipData, decryptedBytes, "decrypted .aes bytes must be identical to the generated .zip file")

		zipChecksum := sha256.Sum256(plainZipData)
		decryptedChecksum := sha256.Sum256(decryptedBytes)
		t.Logf("[Static AES] 5. Verifiziere Prüfsummen-Gleichheit (SHA-256):")
		t.Logf("    - ZIP Checksum:       %x", zipChecksum)
		t.Logf("    - Decrypted Checksum: %x", decryptedChecksum)
		require.Equal(t, zipChecksum, decryptedChecksum, "SHA256 checksum of decrypted .aes must match the .zip file")

		// Verify decrypted zip content
		t.Log("[Static AES] 6. Überprüfe OCFL-Objektstruktur im entschlüsselten ZIP-Archiv...")
		zipReader, err := zip.NewReader(bytes.NewReader(decryptedBytes), int64(len(decryptedBytes)))
		require.NoError(t, err)

		hasInventory := false
		foundFiles := make(map[string]bool)
		for _, f := range zipReader.File {
			foundFiles[f.Name] = true
			if strings.HasSuffix(f.Name, "inventory.json") {
				hasInventory = true
			}
			if f.Name == "0=ocfl_1.1" {
				rc, err := f.Open()
				require.NoError(t, err)
				body, err := io.ReadAll(rc)
				require.NoError(t, err)
				_ = rc.Close()
				require.Contains(t, string(body), "ocfl_1.1")
			}
		}

		require.True(t, foundFiles["0=ocfl_1.1"], "0=ocfl_1.1 must exist in decrypted zip")
		require.True(t, hasInventory, "inventory.json must exist in decrypted zip")
		t.Log("[Static AES] -> OCFL-Konformität (Namaste-Deklaration & inventory.json) erfolgreich validiert")

		// Negative test: Decryption fails with wrong key
		t.Log("[Static AES] 7. Negativtest: Entschlüsselung mit fehlerhaftem KMS-Schlüssel schlägt fehl...")
		wrongClient, err := statickms.NewClient("99999999999999999999999999999999")
		require.NoError(t, err)
		wrongAead, err := wrongClient.GetAEAD("static://")
		require.NoError(t, err)
		_, err = keyset.Read(keyset.NewBinaryReader(bytes.NewBuffer(kStruct.EncryptedKey)), wrongAead)
		require.Error(t, err, "keyset reading should fail with incorrect AES key")
		t.Log("[Static AES] -> Erwarteter Fehler beim Entschlüsseln mit falschem Schlüssel aufgetreten")

		// Negative test: Corrupted ciphertext fails decryption
		t.Log("[Static AES] 8. Negativtest: Manipulation des Chiffretexts führt zu Integritätsfehler...")
		corruptedCiphertext := append([]byte(nil), aesFile...)
		corruptedFile := filepath.Join(tempDir, "corrupted.zip.aes")
		aesBytes, err := os.ReadFile(aesFile)
		require.NoError(t, err)
		if len(aesBytes) > 50 {
			aesBytes[40] ^= 0xFF // flip bits
		}
		require.NoError(t, os.WriteFile(corruptedFile, aesBytes, 0644))

		cFP, err := os.Open(corruptedFile)
		require.NoError(t, err)
		defer cFP.Close()

		cDecReader, err := stream.NewDecryptingReader(cFP, kStruct.Aad)
		require.NoError(t, err)
		_, err = io.ReadAll(cDecReader)
		require.Error(t, err, "reading corrupted ciphertext stream must fail integrity check")
		t.Log("[Static AES] -> Erwarteter Integritätsfehler bei manipuliertem Chiffretext bestätigt")
		_ = corruptedCiphertext
	})

	t.Run("KeePass2 KMS Integration", func(t *testing.T) {
		kdbxPath := filepath.Join(tempDir, "keys.kdbx")
		kdbxPassword := "master-secret-password-123"
		entryTitle := "TestEncKey"
		entryKey := "abcdef0123456789abcdef0123456789" // 32 bytes

		t.Logf("[KeePass2 KMS] 1. Erstelle KeePass2-Datenbank (%s) mit Schlüssel-Eintrag '%s'...", kdbxPath, entryTitle)
		createTestKeePassDB(t, kdbxPath, kdbxPassword, entryTitle, entryKey)

		kdbxEntryURI := "keepass2://" + filepath.Base(kdbxPath) + "/Root/" + entryTitle

		zipPath := filepath.Join(tempDir, "keepass_archive.zip")

		t.Log("[KeePass2 KMS] 2. Erstelle verschlüsselten OCFL-Container über KeePass2 KMS via gocfl create...")
		cmd.ResetForTest()
		rootCmd := cmd.GetRootCmd()
		rootCmd.SetArgs([]string{
			"create",
			zipPath,
			contentDir,
			"--object-id", "id:test-keepass-aes",
			"-m", "encrypted archive with keepass KMS",
			"-u", "KeePass User",
			"-a", "mailto:keepass@example.com",
			"--encrypt-aes",
			"--keepass-file", kdbxPath,
			"--keepass-entry", kdbxEntryURI,
			"--keepass-key", kdbxPassword,
			"--log-level", "DEBUG",
		})

		require.NoError(t, rootCmd.Execute())

		aesFile := zipPath + ".aes"
		keyFile := zipPath + ".aes.key.json"
		t.Logf("[KeePass2 KMS] 3. Verifiziere erzeugte Artefakte (.aes, .aes.key.json): %s", aesFile)
		require.FileExists(t, aesFile)
		require.FileExists(t, keyFile)

		// Decrypt archive using KeePass2 KMS client
		t.Log("[KeePass2 KMS] 4. Lade KeePass2-Datenbank und initialisiere KeePass2-KMS-Client zur Schlüsselentschlüsselung...")
		keyData, err := os.ReadFile(keyFile)
		require.NoError(t, err)

		kStruct := &encrypt.KeyStruct{}
		require.NoError(t, json.Unmarshal(keyData, kStruct))

		db, err := keepass2kms.LoadKeePassDBFromFile(kdbxPath, kdbxPassword)
		require.NoError(t, err)

		client, err := keepass2kms.NewClient(db, filepath.Base(kdbxPath))
		require.NoError(t, err)

		aead, err := client.GetAEAD(kdbxEntryURI)
		require.NoError(t, err)

		kh, err := keyset.Read(keyset.NewBinaryReader(bytes.NewBuffer(kStruct.EncryptedKey)), aead)
		require.NoError(t, err)

		stream, err := streamingaead.New(kh)
		require.NoError(t, err)

		fp, err := os.Open(aesFile)
		require.NoError(t, err)
		defer fp.Close()

		t.Log("[KeePass2 KMS] 5. Entschlüssele Datenstrom aus KeePass2-geschütztem Container...")
		decReader, err := stream.NewDecryptingReader(fp, kStruct.Aad)
		require.NoError(t, err)

		decryptedBytes, err := io.ReadAll(decReader)
		require.NoError(t, err)
		t.Logf("[KeePass2 KMS] -> %d Bytes erfolgreich entschlüsselt", len(decryptedBytes))

		// Verify against unencrypted zip data & checksum
		plainZipData, err := os.ReadFile(zipPath)
		require.NoError(t, err)
		require.Equal(t, plainZipData, decryptedBytes, "decrypted .aes bytes must be identical to the generated .zip file")

		zipChecksum := sha256.Sum256(plainZipData)
		decryptedChecksum := sha256.Sum256(decryptedBytes)
		t.Logf("[KeePass2 KMS] 6. Verifiziere Prüfsummen-Gleichheit (SHA-256):")
		t.Logf("    - ZIP Checksum:       %x", zipChecksum)
		t.Logf("    - Decrypted Checksum: %x", decryptedChecksum)
		require.Equal(t, zipChecksum, decryptedChecksum, "SHA256 checksum of decrypted .aes must match the .zip file")

		// Negative test: Decryption fails with wrong KeePass master password
		t.Log("[KeePass2 KMS] 7. Negativtest: Laden der KeePass-Datenbank mit falschem Master-Passwort schlägt fehl...")
		_, err = keepass2kms.LoadKeePassDBFromFile(kdbxPath, "wrong-password")
		require.Error(t, err, "loading keepass database with invalid password must fail")
		t.Log("[KeePass2 KMS] -> Erwarteter Authentifizierungsfehler beim Zugriff auf KeePass-Datenbank bestätigt")
	})
}
