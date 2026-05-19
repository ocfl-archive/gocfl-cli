package cmd

import (
	"fmt"
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:     "update [path to ocfl structure]",
	Aliases: []string{},
	Short:   "update object in existing ocfl structure",
	Long:    "opens an existing ocfl structure and updates an object. if an object with the given id does not exist, an error is produced",
	Example: "gocfl update ./archive.zip /tmp/testdata -u 'Jane Doe' -a 'mailto:user@domain' -m 'initial add' -object-id 'id:abc123'",
	Args:    cobra.MinimumNArgs(2),
	Run:     doUpdate,
}

func initUpdate() {
	updateCmd.Flags().StringVarP(&flagObjectID, "object-id", "i", "", "object id to update (required)")
	updateCmd.Flags().StringP("message", "m", "", "message for new object version (required)")
	updateCmd.Flags().StringP("user-name", "u", "", "user name for new object version (required)")
	updateCmd.Flags().StringP("user-address", "a", "", "user address for new object version (required)")
	updateCmd.Flags().StringP("digest", "d", "", "digest to use for zip file checksum")
	updateCmd.Flags().Bool("no-deduplicate", false, "disable deduplication (faster)")
	updateCmd.Flags().Bool("echo", false, "update strategy 'echo' (reflects deletions). if not set, update strategy is 'contribute'")
	updateCmd.Flags().Bool("no-compress", false, "do not compress data in zip file")
	updateCmd.Flags().Bool("encrypt-aes", false, "set flag to create encrypted container (only for container target)")
	updateCmd.Flags().String("aes-key", "", "key to use for encrypted container in hex format (64 chars, empty: generate random key)")
	updateCmd.Flags().String("aes-iv", "", "initialisation vector to use for encrypted container in hex format (32 chars, empty: generate random vector)")
}

// doUpdateConf updates the configuration based on the command line flags for the 'update' command.
func doUpdateConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "user-name"); str != "" {
		conf.Update.User.Name = str
	}
	if str := getFlagString(cmd, "user-address"); str != "" {
		conf.Update.User.Address = str
	}
	if str := getFlagString(cmd, "message"); str != "" {
		conf.Update.Message = str
	}
	if str := getFlagString(cmd, "digest"); str != "" {
		conf.Update.Digest = checksum.DigestAlgorithm(str)
	}
	if conf.Update.Digest == "" {
		conf.Update.Digest = checksum.DigestSHA512
	}
	if _, err := checksum.GetHash(conf.Update.Digest); err != nil {
		_ = cmd.Help()
		cobra.CheckErr(errors.Errorf("invalid digest '%s' for flag 'digest' or 'Init.DigestAlgorithm' config file entry", conf.Add.Digest))
	}
	if b, ok := getFlagBool(cmd, "no-deduplicate"); b {
		if ok {
			conf.Update.Deduplicate = !b
		}
	}
	if b, ok := getFlagBool(cmd, "no-compress"); b {
		if ok {
			conf.Update.NoCompress = b
		}
	}
	if b, ok := getFlagBool(cmd, "echo"); b {
		if ok {
			conf.Update.Echo = b
		}
	}

}

// doUpdate is the main function for the 'update' command.
// It opens an existing OCFL structure and updates an existing object with new content.
func doUpdate(cmd *cobra.Command, args []string) {
	ocflPath := args[0]
	srcPath := args[1]

	// Update configuration based on flags
	doUpdateConf(cmd)

	var addr string
	var localCache bool

	fmt.Printf("opening '%s'\n", ocflPath)
	logger.Info().Msgf("opening '%s'", ocflPath)

	if _, err := os.Stat(srcPath); err != nil {
		logger.Fatal().Err(err).Msgf("cannot stat '%s'", srcPath)
	}

	ocflPath = writefs.RealPath(vfs, ocflPath)
	srcPath = writefs.RealPath(vfs, srcPath)

	// Prepare source and destination filesystems
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", srcPath)
	}
	_destFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Fatal().Msgf("filesystem for '%s' is not writable", ocflPath)
	}
	var doNotClose = false
	defer func() {
		if doNotClose {
			logger.Fatal().Msgf("filesystem '%s' not closed", destFS)
		} else {
			if err := writefs.Close(destFS); err != nil {
				logger.Fatal().Err(err).Msgf("error closing filesystem '%s'", destFS)
			}
		}
	}()

	area := conf.DefaultArea
	if area == "" {
		area = "content"
	}
	var areaPaths = map[string]fs.FS{}
	for i := 2; i < len(args); i++ {
		matches := areaPathRegexp.FindStringSubmatch(args[i])
		if matches == nil {
			logger.Error().Msgf("invalid areapath '%s'", args[i])
			continue
		}
		areaPaths[matches[1]], err = writefs.Sub(vfs, matches[2])
		if err != nil {
			doNotClose = true
			logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", args[i])
		}
	}

	ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
	ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
	ext_NNNN_metafile.Init(vfs, logger)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot get extension params")
	}

	// Setup extension managers for storage root and object
	_, _, err = initocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder)), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		doNotClose = true
		return
	}

	// Load the storage root
	storageRoot, srCloser, err := initocfl.LoadStorageRoot(ctx, destFS, extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		doNotClose = true
		return
	}
	defer srCloser.Close()

	exists, err := storageRoot.ObjectExists(flagObjectID)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot check for object '%s'", flagObjectID)
		doNotClose = true
		return
	}
	if !exists {
		fmt.Printf("Object '%s' does not exists, exiting", flagObjectID)
		doNotClose = true
		return
	}

	// Add/Update the object in the storage root
	_, err = addObjectByPath(
		ctx,
		storageRoot,
		nil,
		nil,
		conf.Update.Deduplicate,
		flagObjectID,
		conf.Update.User.Name,
		conf.Update.User.Address,
		conf.Update.Message,
		sourceFS,
		nil,
		area,
		areaPaths,
		conf.Update.Echo,
		logger,
	)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot write content to storageroot filesystem '%s'", destFS)
		doNotClose = true
	}
	_ = showStatus(logger)

}
