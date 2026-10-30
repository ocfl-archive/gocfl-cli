package tests

import (
	"archive/zip"
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	statickms "github.com/je4/utils/v2/pkg/StaticKMS"
	"github.com/je4/utils/v2/pkg/encrypt"
	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/streamingaead"
)

func TestCreateZipAESAndDecrypt(t *testing.T) {
	tempDir := t.TempDir()
	contentDir := filepath.Join(tempDir, "content")
	require.NoError(t, os.MkdirAll(contentDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(contentDir, "test.txt"), []byte("hello encrypted world"), 0644))

	zipPath := filepath.Join(tempDir, "test_archive.zip")
	aesKey := "01234567890123456789012345678901" // 32 bytes

	cmd.ResetForTest()
	rootCmd := cmd.GetRootCmd()
	rootCmd.SetArgs([]string{
		"create",
		zipPath,
		contentDir,
		"--config", "internal",
		"--object-id", "id:test-aes-object",
		"-m", "initial commit",
		"-u", "Test User",
		"-a", "mailto:test@example.com",
		"--encrypt-aes",
		"--aes-key", aesKey,
		"--log-level", "DEBUG",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	// Verify plain zip was created
	plainZipData, err := os.ReadFile(zipPath)
	require.NoError(t, err)
	require.NotEmpty(t, plainZipData)
	zr, err := zip.OpenReader(zipPath)
	require.NoError(t, err)
	_ = zr.Close()

	// Verify plain zip checksum file was created and matches plain zip
	plainChecksumFile := zipPath + ".sha512"
	plainChecksumData, err := os.ReadFile(plainChecksumFile)
	require.NoError(t, err)
	hasher := sha512.New()
	hasher.Write(plainZipData)
	expectedPlainDigest := hex.EncodeToString(hasher.Sum(nil))
	require.True(t, strings.HasPrefix(string(plainChecksumData), expectedPlainDigest+" *"))
	require.True(t, strings.HasSuffix(string(plainChecksumData), filepath.Base(zipPath)))

	// Verify .zip.aes and .zip.aes.key.json were created
	aesFile := zipPath + ".aes"
	keyFile := zipPath + ".aes.key.json"

	aesData, err := os.ReadFile(aesFile)
	require.NoError(t, err)
	require.NotEmpty(t, aesData)

	// Verify .zip.aes.sha512 checksum file was created and matches aes ciphertext
	aesChecksumFile := zipPath + ".aes.sha512"
	aesChecksumData, err := os.ReadFile(aesChecksumFile)
	require.NoError(t, err)
	aesHasher := sha512.New()
	aesHasher.Write(aesData)
	expectedAesDigest := hex.EncodeToString(aesHasher.Sum(nil))
	require.True(t, strings.HasPrefix(string(aesChecksumData), expectedAesDigest+" *"))
	require.True(t, strings.HasSuffix(string(aesChecksumData), filepath.Base(zipPath)+".aes"))

	keyData, err := os.ReadFile(keyFile)
	require.NoError(t, err)
	require.NotEmpty(t, keyData)

	// Verify decryption
	kStruct := &encrypt.KeyStruct{}
	err = json.Unmarshal(keyData, kStruct)
	require.NoError(t, err)

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

	decReader, err := stream.NewDecryptingReader(fp, kStruct.Aad)
	require.NoError(t, err)

	decryptedBytes, err := io.ReadAll(decReader)
	require.NoError(t, err)

	require.Equal(t, plainZipData, decryptedBytes)
}
