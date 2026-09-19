package cmd

import (
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:     "init [path to ocfl structure]",
	Aliases: []string{},
	Short:   "initializes an empty ocfl structure",
	Long:    "initializes an empty ocfl structure",
	Example: "gocfl init ./archive.zip",
	Args:    cobra.ExactArgs(1),
	RunE:    doInit,
}

func initInit() {
	initCmd.Flags().String("default-storageroot-extensions", "", "folder with initial extension configurations for new OCFL Storage Root")
	initCmd.Flags().String("ocfl-version", "", "ocfl version for new storage root")
	initCmd.Flags().StringP("digest", "d", "", "digest to use for ocfl checksum")
	initCmd.Flags().Bool("no-compress", false, "do not compress data in zip file")
}

// doInitConf updates the configuration based on the command line flags for the 'init' command.
func doInitConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "default-storageroot-extensions"); str != "" {
		if err := conf.Init.StorageRootExtensionFolder.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid default-storageroot-extensions '%s' for flag 'default-storageroot-extensions' or 'Init.StorageRootExtensionFolder' config file entry", str)
			return errors.Wrapf(err, "invalid default-storageroot-extensions '%s'", str)
		}
	}

	if str := getFlagString(cmd, "ocfl-version"); str != "" {
		conf.Init.OCFLVersion = str
	}

	if str := getFlagString(cmd, "digest"); str != "" {
		conf.Init.Digest = checksum.DigestAlgorithm(str)
	}
	if conf.Init.Digest != "" {
		if _, err := checksum.GetHash(conf.Init.Digest); err != nil {
			_ = cmd.Help()
			return errors.Errorf("invalid digest '%s' for flag 'digest' or 'Init.DigestAlgorithm' config file entry", conf.Init.Digest)
		}
	}
	return nil
}

// doInit is the main function for the 'init' command.
// It initializes a new, empty OCFL storage root at the specified path.
func doInit(cmd *cobra.Command, args []string) error {
	ocflPath := args[0]

	// Update configuration based on flags
	if err := doInitConf(cmd); err != nil {
		return err
	}

	logger.Info().Msgf("creating '%s'", ocflPath)

	ocflPath = writefs.RealPath(vfs, ocflPath)

	// Prepare access to the OCFL directory
	_destFS, err := writefs.SubCreate(vfs, ocflPath)
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
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
	}()

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return errors.Wrap(err, "cannot get extension params")
	}

	// Create the storage root
	if _, err := CreateStorageRoot(
		ctx,
		destFS,
		firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder.String())),
		version.OCFLVersion(conf.Init.OCFLVersion),
		conf.Init.Digest,
		extensionParams, logger,
	); err != nil {
		logger.Error().Err(err).Msgf("cannot create new storageroot")
		return errors.Wrap(err, "cannot create new storageroot")
	}

	if showStatus(logger) {
		return errors.New("init failed")
	}
	return nil
}
