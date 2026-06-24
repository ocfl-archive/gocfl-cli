package cmd

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"emperror.dev/errors"
	statickms "github.com/je4/utils/v2/pkg/StaticKMS"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/keepass2kms"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfsw"
	"github.com/ocfl-archive/filesystem/pkg/zipfswenc"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/spf13/cobra"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

var createCmd = &cobra.Command{
	Use:     "create [path to ocfl structure] [path to content folder]",
	Aliases: []string{},
	Short:   "creates a new ocfl structure with initial content of one object",
	Long: "initializes an empty ocfl structure and adds contents of a directory subtree to it\n" +
		"This command is a combination of init and add",
	Example: "gocfl create ./archive.zip /tmp/testdata --digest sha512 -u 'Jane Doe' -a 'mailto:user@domain' -m 'initial add' -object-id 'id:abc123'",
	Args:    cobra.MinimumNArgs(2),
	RunE:    doCreate,
}

// initCreate initializes the gocfl create command
func initCreate() {
	createCmd.Flags().String("default-storageroot-extensions", "", "folder with initial extension configurations for new OCFL Storage Root")
	createCmd.Flags().String("ocfl-version", "1.1", "ocfl version for new storage root")
	createCmd.Flags().StringVarP(&flagObjectID, "object-id", "i", "", "object id to update (required)")
	createCmd.MarkFlagRequired("object-id")
	createCmd.Flags().String("default-object-extensions", "", "folder with initial extension configurations for new OCFL objects")
	createCmd.Flags().StringP("message", "m", "", "message for new object version (required)")
	createCmd.Flags().StringP("user-name", "u", "", "user name for new object version (required)")
	createCmd.Flags().StringP("user-address", "a", "", "user address for new object version (required)")
	createCmd.Flags().StringP("fixity", "f", "", fmt.Sprintf("comma separated list of digest algorithms for fixity %v", checksum.DigestNames))
	createCmd.Flags().StringP("digest", "d", "", "digest to use for ocfl checksum")
	createCmd.Flags().String("default-area", "", "default area for update or ingest (default: content)")
	createCmd.Flags().Bool("deduplicate", false, "force deduplication (slower)")
	createCmd.Flags().Bool("no-compress", false, "do not compress data in zip file")
	createCmd.Flags().Bool("encrypt-aes", false, "create encrypted container (only for container target)")
	createCmd.Flags().String("aes-key", "", "key to use for encrypted container in hex format (64 chars, empty: generate random key)")
	createCmd.Flags().String("aes-iv", "", "initialisation vector to use for encrypted container in hex format (32 chars, empty: generate random vector)")
	createCmd.Flags().String("keepass-file", "", "file with keypass2 database")
	createCmd.Flags().String("keepass-entry", "", "keypass2 entry to use for key encryption")
	createCmd.Flags().String("keepass-key", "", "key to use for keypass2 database decryption")
}

func isEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func doCreateConf(cmd *cobra.Command) {
	if b, ok := getFlagBool(cmd, "encrypt-aes"); ok {
		conf.AES.Enable = b
	}
	if str := getFlagString(cmd, "aes-key"); str != "" {
		if err := conf.AES.Key.UnmarshalText(([]byte)(str)); err != nil {
			logger.Error().Err(err).Msg("cannot parse aes-key")
		}
	}
	if str := getFlagString(cmd, "aes-iv"); str != "" {
		if err := conf.AES.IV.UnmarshalText(([]byte)(str)); err != nil {
			logger.Error().Err(err).Msg("cannot parse aes-iv")
		}
	}
	if str := getFlagString(cmd, "keepass-file"); str != "" {
		if err := conf.AES.KeepassFile.UnmarshalText(([]byte)(str)); err != nil {
			logger.Error().Err(err).Msg("cannot parse keepass-file")
		}
	}
	if str := getFlagString(cmd, "keepass-entry"); str != "" {
		if err := conf.AES.KeepassEntry.UnmarshalText(([]byte)(str)); err != nil {
			logger.Error().Err(err).Msg("cannot parse keepass-entry")
		}
	}
	if str := getFlagString(cmd, "keepass-key"); str != "" {
		if err := conf.AES.KeepassKey.UnmarshalText(([]byte)(str)); err != nil {
			logger.Error().Err(err).Msg("cannot parse keepass-key")
		}
	}
}

// doCreate is the main function for the 'create' command.
// It initializes a new OCFL storage root and adds an initial object to it.
// This command effectively combines 'init' and 'add' operations.
func doCreate(cmd *cobra.Command, args []string) error {
	// Validate that all required flags are set
	if err := cmd.ValidateRequiredFlags(); err != nil {
		return errors.WithStack(err)
	}

	ocflPath := args[0]
	srcPath := args[1]

	// Update internal configuration based on provided flags
	doInitConf(cmd)
	doAddConf(cmd)
	doCreateConf(cmd)

	ocflPath = writefs.RealPath(vfs, ocflPath)
	srcPath = writefs.RealPath(vfs, srcPath)
	logger.Info().Msgf("creating '%s'", ocflPath)

	fmt.Printf("creating '%s'\n", ocflPath)

	// Collect fixity algorithms from configuration
	var fixityAlgs = []checksum.DigestAlgorithm{}
	for _, alg := range conf.Add.Fixity {
		alg = strings.TrimSpace(strings.ToLower(alg))
		if alg == "" {
			continue
		}
		if _, err := checksum.GetHash(checksum.DigestAlgorithm(alg)); err != nil {
			return errors.Errorf("unknown hash function '%s'", alg)
		}
		fixityAlgs = append(fixityAlgs, checksum.DigestAlgorithm(alg))
	}

	// Check if the target is a ZIP file based on the extension
	isZip := strings.HasSuffix(strings.ToLower(ocflPath), ".zip")
	fi, err := fs.Stat(vfs, ocflPath)
	if err == nil {
		// If target exists, it must be a directory and it must be empty
		if isZip || !fi.IsDir() {
			return ErrorFactory.NewError(ERRORTest2, "already exists", errors.Errorf("file '%s' already exists", ocflPath))
		}

		if empty, err := isEmpty(ocflPath); err != nil {
			return errors.Wrapf(err, "cannot check if directory '%s' is empty", ocflPath)
		} else if !empty {
			return errors.Errorf("directory '%s' is not empty", ocflPath)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return errors.Wrapf(err, "cannot check status of '%s'", ocflPath)
	}

	// Prepare source and destination filesystems
	var _destFS fs.FS
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		return errors.Wrapf(err, "cannot get filesystem for '%s'", srcPath)
	}

	var zipWriter io.WriteCloser
	if isZip {
		if conf.AES.Enable {
			var client registry.KMSClient
			if conf.AES.Key != "" {
				logger.Info().Msgf("using static KMS client")
				client, err = statickms.NewClient(string(conf.AES.Key))
				if err != nil {
					return errors.Wrap(err, "cannot create static KMS client")
				}

			} else {
				logger.Info().Msgf("using keepass2kms client with file '%s'", conf.AES.KeepassFile)
				db, err := keepass2kms.LoadKeePassDBFromFile(string(conf.AES.KeepassFile), string(conf.AES.KeepassKey))
				if err != nil {
					return errors.Wrapf(err, "cannot load keepass2kms database from file '%s'", conf.AES.KeepassFile)
				}

				entryPath := conf.AES.KeepassEntry.String()
				prefix := "keepass2://" + filepath.Base(string(conf.AES.KeepassFile)) + "/"
				if strings.HasPrefix(entryPath, prefix) {
					entryPath = entryPath[len(prefix):]
				}

				entry := keepass2kms.GetEntry(db.Content.Root, entryPath, false)
				if entry == nil {
					return errors.Errorf("key %s not found in keepass2kms database '%s'", conf.AES.KeepassEntry, conf.AES.KeepassFile)
				}
				key := entry.GetPassword()
				if len(key) != 32 {
					return errors.Errorf("key %s in keepass2kms database '%s' has wrong length (expected 32, got %d)", conf.AES.KeepassEntry, conf.AES.KeepassFile, len(key))
				}
				key = ""
				_ = key

				client, err = keepass2kms.NewClient(db, filepath.Base(string(conf.AES.KeepassFile)))
				if err != nil {
					return errors.Wrap(err, "cannot create keepass2kms client")
				}
			}
			registry.RegisterKMSClient(client)
			// create encrypted file
			_destFS, err = zipfswenc.NewFSFileChecksumsEncrypted(
				vfs,
				ocflPath,
				true,
				[]checksum.DigestAlgorithm{conf.Init.Digest},
				conf.AES.KeepassEntry.String(),
				logger.Logger(),
			)
			if err != nil {
				return errors.Wrapf(err, "cannot create encrypted zip file '%s'", ocflPath)
			}
		} else {
			// Create a writer for the ZIP file
			zipWriter, err = writefs.Create(vfs, ocflPath)
			_destFS, err = zipfsw.NewFS(
				zipWriter,
				true, // closeWriter: zipfsw will close zipWriter when closed
				true, // noCompression: as per requirements or config
				path.Base(ocflPath),
				[]checksum.DigestAlgorithm{conf.Init.Digest},
				func(css map[checksum.DigestAlgorithm]string) error {
					// Callback to write checksum files after ZIP is closed
					if css == nil {
						return errors.Errorf("checksum of '%s' cannot be nil", ocflPath)
					}
					for alg, digest := range css {
						if _, err := writefs.WriteFile(vfs, ocflPath+"."+alg.String(), []byte(fmt.Sprintf("%s *%s", digest, path.Base(ocflPath)))); err != nil {
							logger.Error().Err(err).Msgf("cannot write checksum file '%s.%s'", ocflPath, alg.String())
						}
					}
					return nil
				},
				logger.Logger(),
			)
			if err != nil {
				// If ZIP FS creation fails, try to close zipWriter if possible
				if closer, ok := zipWriter.(io.Closer); ok {
					_ = closer.Close()
				}
				return errors.Wrapf(err, "cannot create zip filesystem for '%s'", ocflPath)
			}
		}
	} else {
		// Regular directory-based filesystem
		_destFS, err = writefs.SubCreate(vfs, ocflPath)
		if err != nil {
			return errors.Wrapf(err, "cannot get filesystem for '%s'", ocflPath)
		}
	}

	// Ensure destination filesystem is writeable
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		return errors.Errorf("filesystem for '%s' is not writeable", ocflPath)
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
	}()

	// Initialize extensions
	var localCache bool
	ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
	ext_NNNN_indexer.Init(conf.Indexer, localCache, logger)
	ext_NNNN_metafile.Init(vfs, logger)

	// Determine default area for objects
	area := conf.DefaultArea
	if area == "" {
		area = "content"
	}

	// Handle additional area paths from arguments
	var areaPaths = map[string]fs.FS{}
	for i := 2; i < len(args); i++ {
		matches := areaPathRegexp.FindStringSubmatch(args[i])
		if matches == nil {
			logger.Error().Msgf("no area given in areapath '%s'", args[i])
			continue
		}
		p := matches[2]
		p = writefs.RealPath(vfs, p)
		areaPaths[matches[1]], err = writefs.Sub(vfs, p)
		if err != nil {
			return errors.Wrapf(err, "cannot get filesystem for '%s'", args[i])
		}
	}

	// Retrieve extension parameters from command flags
	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		return errors.Wrap(err, "cannot get extension params")
	}

	// Create the OCFL storage root
	extensionFS := firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder.String()))
	storageRoot, err := CreateStorageRoot(
		ctx,
		destFS,
		extensionFS,
		version.OCFLVersion(conf.Init.OCFLVersion),
		conf.Init.Digest,
		extensionParams,
		logger,
	)
	if err != nil {
		return errors.Wrap(err, "cannot create new storage root")
	}
	defer storageRoot.Close()

	// Determine folder path for the new object based on its ID
	objPath, err := storageRoot.IdToFolder(flagObjectID)
	if err != nil {
		return errors.Wrapf(err, "cannot create folder for id %s", flagObjectID)
	}

	// Get a sub-filesystem for the object
	objectFS, closer, err := appendfs.Sub(storageRoot.GetWriteFS(), objPath)
	if err != nil {
		return errors.Wrapf(err, "cannot create subfs %v / %s for id %s", storageRoot.GetWriteFS(), objPath, flagObjectID)
	}
	// Use defer for closer as it is local and non-critical for overall archive integrity if it fails later
	defer func() {
		if err := closer.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close object subfs")
		}
	}()

	// Initialize the OCFL object
	objectExtensionFS := firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder.String()))
	o, err := ocfl.InitObject(ctx, objectFS, objectExtensionFS, storageRoot.GetOCFLVersion(), flagObjectID, storageRoot.GetDigest(), extensionParams, logger)
	if err != nil {
		return errors.Wrapf(err, "cannot create object %s", flagObjectID)
	}
	// Object should be closed at the end to flush its inventory
	defer func() {
		if err := o.Close(); err != nil {
			logger.Error().Err(err).Msgf("cannot close object %s", flagObjectID)
		}
	}()

	// Start a new version for the object
	versionWriter, err := o.StartUpdate(conf.Add.Message, conf.Add.User.Name, conf.Add.User.Address, false)
	if err != nil {
		return errors.Wrapf(err, "cannot start update for object %s", flagObjectID)
	}

	// Add the main source folder to the new version
	if err := versionWriter.AddFolder(sourceFS, conf.Add.Deduplicate, area); err != nil {
		_ = versionWriter.Close()
		return errors.Wrapf(err, "cannot add folder '%s' to '%s'", sourceFS, flagObjectID)
	}

	// Add any additional area folders
	if areaPaths != nil {
		for a, aPath := range areaPaths {
			if err := versionWriter.AddFolder(aPath, conf.Add.Deduplicate, a); err != nil {
				_ = versionWriter.Close()
				return errors.Wrapf(err, "cannot add area '%s' folder '%s' to '%s'", a, aPath, flagObjectID)
			}
		}
	}

	// Finalize the version
	if err := versionWriter.Close(); err != nil {
		return errors.Wrapf(err, "cannot close version writer for object %s", flagObjectID)
	}

	// Show result status
	_ = showStatus(logger)

	return nil
}
