package cmd

import (
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
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
	Run:     doInit,
}

func initInit() {
	initCmd.Flags().String("default-storageroot-extensions", "", "folder with initial extension configurations for new OCFL Storage Root")
	initCmd.Flags().String("ocfl-version", "", "ocfl version for new storage root")
	initCmd.Flags().StringP("digest", "d", "", "digest to use for ocfl checksum")
	initCmd.Flags().Bool("no-compress", false, "do not compress data in zip file")
}

// doInitConf updates the configuration based on the command line flags for the 'init' command.
func doInitConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "default-storageroot-extensions"); str != "" {
		conf.Init.StorageRootExtensionFolder = str
	}

	if str := getFlagString(cmd, "ocfl-version"); str != "" {
		conf.Init.OCFLVersion = str
	}

	if str := getFlagString(cmd, "digest"); str != "" {
		conf.Init.Digest = checksum.DigestAlgorithm(str)
	}
	if _, err := checksum.GetHash(conf.Init.Digest); err != nil {
		_ = cmd.Help()
		cobra.CheckErr(errors.Errorf("invalid digest '%s' for flag 'digest' or 'Init.DigestAlgorithm' config file entry", conf.Init.Digest))
	}

}

// doInit is the main function for the 'init' command.
// It initializes a new, empty OCFL storage root at the specified path.
func doInit(cmd *cobra.Command, args []string) {
	ocflPath := args[0]

	// Update configuration based on flags
	doInitConf(cmd)

	logger.Info().Msgf("creating '%s'", ocflPath)

	ocflPath = writefs.RealPath(vfs, ocflPath)

	// Prepare access to the OCFL directory
	_destFS, err := writefs.SubCreate(vfs, ocflPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
		return
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Error().Msgf("filesystem for '%s' is not writable", ocflPath)
		return
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
	}()

	// Setup extension factory and manager for storage root
	storageRootExtensionFactory, err := setupExtensionFactory[storageroot.ExtensionManager](cmd, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Factory fail")
		return
	}
	storageRootExtensionManager, err := LoadExtensionManager(
		storageRootExtensionFactory,
		firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder)),
	)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root extension")
		return
	}
	defer func() {
		if err := storageRootExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Create the storage root
	if _, err := CreateStorageRoot(
		ctx,
		destFS,
		version.OCFLVersion(conf.Init.OCFLVersion),
		storageRootExtensionFactory, storageRootExtensionManager,
		conf.Init.Digest,
		(logger),
	); err != nil {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
		logger.Error().Err(err).Msgf("cannot create new storageroot")
		return
	}

	_ = showStatus(logger)
}
