package cmd

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
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
	if err := cmd.ValidateRequiredFlags(); err != nil {
		cobra.CheckErr(err)
		return
	}

	ocflPath := args[0]
	srcPath := args[1]

	// Update configuration based on flags
	doInitConf(cmd)
	doAddConf(cmd)

	ocflPath = writefs.RealPath(vfs, ocflPath)
	srcPath = writefs.RealPath(vfs, srcPath)
	logger.Info().Msgf("creating '%s'", ocflPath)

	fmt.Printf("creating '%s'\n", ocflPath)

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

	if fi, err := os.Stat(ocflPath); err == nil {
		if fi.IsDir() {
			if empty, err := isEmpty(ocflPath); err != nil {
				logger.Error().Err(err).Msgf("cannot check if directory '%s' is empty", ocflPath)
				return
			} else if !empty {
				logger.Error().Msgf("directory '%s' is not empty", ocflPath)
				return
			}
		} else {
			logger.Error().
				Any("archive_error", ErrorFactory.NewError(ERRORTest2, "already exists", nil)).
				Msgf("'%s' already exists and is not an empty directory", ocflPath)
			return
		}
	}

	// Prepare source and destination filesystems
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", srcPath)
	}
	_destFS, err := writefs.SubCreate(vfs, ocflPath)
	if err != nil {
		logger.Fatal().Msgf("cannot get filesystem for '%s'", ocflPath)
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Fatal().Msgf("filesystem for '%s' is not writeable", ocflPath)
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Fatal().Err(err).Msgf("error closing filesystem '%s'", destFS)
		}
	}()

	var addr string
	var localCache bool
	ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
	ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
	ext_NNNN_metafile.Init(vfs, logger)

	area := conf.DefaultArea
	if area == "" {
		area = "content"
	}
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
			logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", args[i])
		}
	}

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot get extension params")
	}

	// Create the storage root
	storageRoot, err := CreateStorageRoot(
		ctx,
		destFS,
		firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder)),
		version.OCFLVersion(conf.Init.OCFLVersion),
		conf.Init.Digest,
		extensionParams,
		logger,
	)
	if err != nil {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Fatal().Err(err).Msg("cannot create new storage root")
	}

	// Add the object to the storage root
	_, err = addObjectByPath(
		ctx,
		storageRoot,
		fixityAlgs,
		extensionParams,
		conf.Add.Deduplicate,
		flagObjectID,
		conf.Add.User.Name,
		conf.Add.User.Address,
		conf.Add.Message,
		sourceFS,
		firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)),
		area,
		areaPaths,
		false,
		logger,
	)
	if err != nil {
		logger.Fatal().Err(err).Msgf("error adding content to storageroot filesystem '%s'", destFS)
	}
	_ = showStatus(logger)

}
