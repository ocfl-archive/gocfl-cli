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
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
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
	RunE:    doUpdate,
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
func doUpdateConf(cmd *cobra.Command) error {
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
		return errors.Errorf("invalid digest '%s' for flag 'digest' or 'Update.Digest' config file entry", conf.Update.Digest)
	}
	if b, ok := getFlagBool(cmd, "no-deduplicate"); ok {
		conf.Update.Deduplicate = !b
	}
	if b, ok := getFlagBool(cmd, "no-compress"); ok {
		conf.Update.NoCompress = b
	}
	if b, ok := getFlagBool(cmd, "echo"); ok {
		conf.Update.Echo = b
	}
	return nil
}

// doUpdate is the main function for the 'update' command.
// It opens an existing OCFL structure and updates an existing object with new content.
func doUpdate(cmd *cobra.Command, args []string) error {
	if err := cmd.ValidateRequiredFlags(); err != nil {
		return errors.WithStack(err)
	}

	ocflPath := args[0]
	srcPath := args[1]

	// Update configuration based on flags
	if err := doUpdateConf(cmd); err != nil {
		return err
	}

	var localCache bool

	fmt.Printf("opening '%s'\n", ocflPath)
	logger.Info().Msgf("opening '%s'", ocflPath)

	if _, err := fs.Stat(vfs, srcPath); err != nil {
		logger.Error().Err(err).Msgf("cannot stat '%s'", srcPath)
		return errors.Wrapf(err, "cannot stat '%s'", srcPath)
	}

	ocflPath = writefs.RealPath(vfs, ocflPath)
	srcPath = writefs.RealPath(vfs, srcPath)

	// Prepare source and destination filesystems
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", srcPath)
		return errors.Wrapf(err, "cannot get filesystem for '%s'", srcPath)
	}
	_destFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
		return errors.Wrapf(err, "cannot get filesystem for '%s'", ocflPath)
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Error().Msgf("filesystem for '%s' is not writable", ocflPath)
		return errors.Errorf("filesystem for '%s' is not writable", ocflPath)
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("error closing filesystem '%s'", destFS)
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
			logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", args[i])
			return errors.Wrapf(err, "cannot get filesystem for '%s'", args[i])
		}
	}

	ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
	ext_NNNN_indexer.Init(conf.Indexer, localCache, logger)
	ext_NNNN_metafile.Init(vfs, logger)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return errors.Wrap(err, "cannot get extension params")
	}

	// Setup extension managers for storage root and object
	storageRootExtensionManager, _, err := ocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder.String())), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return errors.Wrap(err, "cannot setup storage root extension manager")
	}
	defer func() {
		if err := storageRootExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Load the storage root
	storageRoot, err := ocfl.LoadStorageRoot(ctx, destFS, extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return errors.Wrap(err, "cannot load storage root")
	}
	defer storageRoot.Close()

	exists, err := storageRoot.ObjectExists(flagObjectID)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot check for object '%s'", flagObjectID)
		return errors.Wrapf(err, "cannot check for object '%s'", flagObjectID)
	}
	if !exists {
		fmt.Printf("Object '%s' does not exists, exiting\n", flagObjectID)
		logger.Error().Msgf("object '%s' does not exist", flagObjectID)
		return errors.Errorf("object '%s' does not exist", flagObjectID)
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
		return errors.Wrapf(err, "cannot write content to storageroot filesystem '%s'", destFS)
	}
	if showStatus(logger) {
		return errors.New("update failed")
	}
	return nil
}
