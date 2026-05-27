package cmd

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfsw"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:     "create [path to ocfl structure] [path to content folder]",
	Aliases: []string{},
	Short:   "creates a new ocfl structure with initial content of one object",
	Long: "initializes an empty ocfl structure and adds contents of a directory subtree to it\n" +
		"This command is a combination of init and add",
	Example: "gocfl create ./archive.zip /tmp/testdata --digest sha512 -u 'Jane Doe' -a 'mailto:user@domain' -m 'initial add' -object-id 'id:abc123'",
	Args:    cobra.MinimumNArgs(2),
	Run:     doCreate,
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
	createCmd.Flags().String("keypass-file", "", "file with keypass2 database")
	createCmd.Flags().String("keypass-entry", "", "keypass2 entry to use for key encryption")
	createCmd.Flags().String("keypass-key", "", "key to use for keypass2 database decryption")
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

// doCreate is the main function for the 'create' command.
// It initializes a new OCFL storage root and adds an initial object to it.
// This command effectively combines 'init' and 'add' operations.
func doCreate(cmd *cobra.Command, args []string) {
	// Validate that all required flags are set
	if err := cmd.ValidateRequiredFlags(); err != nil {
		cobra.CheckErr(err)
		return
	}

	ocflPath := args[0]
	srcPath := args[1]

	// Update internal configuration based on provided flags
	doInitConf(cmd)
	doAddConf(cmd)

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
			logger.Error().Msgf("unknown hash function '%s'", alg)
			return
		}
		fixityAlgs = append(fixityAlgs, checksum.DigestAlgorithm(alg))
	}

	// Check if the target is a ZIP file based on the extension
	isZip := strings.HasSuffix(strings.ToLower(ocflPath), ".zip")
	fi, err := fs.Stat(vfs, ocflPath)
	if err == nil {
		// If target exists, it must be a directory and it must be empty
		if isZip || !fi.IsDir() {
			logger.Error().
				Any("archive_error", ErrorFactory.NewError(ERRORTest2, "already exists", nil)).
				Msgf("file '%s' already exists", ocflPath)
			return
		}

		if empty, err := isEmpty(ocflPath); err != nil {
			logger.Error().Err(err).Msgf("cannot check if directory '%s' is empty", ocflPath)
			return
		} else if !empty {
			logger.Error().Msgf("directory '%s' is not empty", ocflPath)
			return
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		logger.Error().Err(err).Msgf("cannot check status of '%s'", ocflPath)
		return
	}

	// Prepare source and destination filesystems
	var _destFS fs.FS
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", srcPath)
	}

	if isZip {
		// Create a writer for the ZIP file
		zipWriter, err := writefs.Create(vfs, ocflPath)
		if err != nil {
			logger.Fatal().Err(err).Msgf("cannot create zip file '%s'", ocflPath)
		}
		// Initialize the ZIP filesystem wrapper
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
			logger.Fatal().Err(err).Msgf("cannot create zip filesystem for '%s'", ocflPath)
		}
	} else {
		// Regular directory-based filesystem
		_destFS, err = writefs.SubCreate(vfs, ocflPath)
		if err != nil {
			logger.Fatal().Msgf("cannot get filesystem for '%s'", ocflPath)
		}
	}

	// Ensure destination filesystem is writeable
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Fatal().Msgf("filesystem for '%s' is not writeable", ocflPath)
	}

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
		path := matches[2]
		path = writefs.RealPath(vfs, path)
		areaPaths[matches[1]], err = writefs.Sub(vfs, path)
		if err != nil {
			if err := writefs.Close(destFS); err != nil {
				logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
			}
			logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", args[i])
		}
	}

	// Retrieve extension parameters from command flags
	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msg("cannot get extension params")
	}

	// Create the OCFL storage root
	extensionFS := firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder))
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
		// Ensure filesystem is closed before fatal exit
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Error().Err(err).Msg("cannot create new storage root")
		return
	}

	// Determine folder path for the new object based on its ID
	objPath, err := storageRoot.IdToFolder(flagObjectID)
	if err != nil {
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot create folder for id %s", flagObjectID)
	}

	// Get a sub-filesystem for the object
	objectFS, closer, err := appendfs.Sub(storageRoot.GetWriteFS(), objPath)
	if err != nil {
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot create subfs %v / %s for id %s", storageRoot.GetWriteFS(), objPath, flagObjectID)
	}
	// Use defer for closer as it is local and non-critical for overall archive integrity if it fails later
	defer func() {
		if err := closer.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close object subfs")
		}
	}()

	// Initialize the OCFL object
	objectExtensionFS := firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder))
	o, err := ocfl.InitObject(ctx, objectFS, objectExtensionFS, storageRoot.GetOCFLVersion(), flagObjectID, storageRoot.GetDigest(), extensionParams, logger)
	if err != nil {
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot create object %s", flagObjectID)
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
		_ = o.Close()
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot start update for object %s", flagObjectID)
	}

	// Add the main source folder to the new version
	if err := versionWriter.AddFolder(sourceFS, conf.Add.Deduplicate, area); err != nil {
		_ = versionWriter.Close()
		_ = o.Close()
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot add folder '%s' to '%s'", sourceFS, flagObjectID)
	}

	// Add any additional area folders
	if areaPaths != nil {
		for a, aPath := range areaPaths {
			if err := versionWriter.AddFolder(aPath, conf.Add.Deduplicate, a); err != nil {
				_ = versionWriter.Close()
				_ = o.Close()
				storageRoot.Close()
				if err := writefs.Close(destFS); err != nil {
					logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
				}
				logger.Fatal().Err(err).Msgf("cannot add area '%s' folder '%s' to '%s'", a, aPath, flagObjectID)
			}
		}
	}

	// Finalize the version
	if err := versionWriter.Close(); err != nil {
		_ = o.Close()
		storageRoot.Close()
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msgf("cannot close version writer for object %s", flagObjectID)
	}

	// Cleanup and close all resources in correct order
	if err := o.Close(); err != nil {
		logger.Error().Err(err).Msgf("error closing object %s", flagObjectID)
	}
	storageRoot.Close()
	if err := writefs.Close(destFS); err != nil {
		logger.Error().Err(err).Msgf("error closing filesystem '%s'", destFS)
	}

	// Show result status
	_ = showStatus(logger)
}
