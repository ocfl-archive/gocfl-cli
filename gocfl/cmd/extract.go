package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/je4/filesystem/v4/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/functions"
	inventorytypes "github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var extractCmd = &cobra.Command{
	Use:     "extract [path to ocfl storage root] [path to target folder]",
	Aliases: []string{},
	Short:   "extract version of ocfl content",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl extract ./archive.zip /tmp/archive",
	Args:    cobra.MinimumNArgs(2),
	Run:     doExtract,
}

func initExtract() {
	extractCmd.Flags().StringP("object-path", "p", "", "object path to extract")
	extractCmd.Flags().StringP("object-id", "i", "", "object id to extract")
	extractCmd.Flags().Bool("with-manifest", false, "generate manifest file in object extraction folder")
	extractCmd.Flags().String("version", "", "version to extract")
	extractCmd.Flags().String("area", "content", "data area to extract")
}

// doExtractConf updates the configuration based on the command line flags for the 'extract' command.
func doExtractConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "object-path"); str != "" {
		conf.Extract.ObjectPath = str
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.Extract.ObjectID = str
	}
	if b, ok := getFlagBool(cmd, "with-manifest"); b {
		if ok {
			conf.Extract.Manifest = b
		}
	}
	if str := getFlagString(cmd, "version"); str != "" {
		conf.Extract.Version = str
	}
	if str := getFlagString(cmd, "area"); str != "" {
		conf.Extract.Area = str
	}
	if conf.Extract.Version == "" {
		conf.Extract.Version = "latest"
	}
}

// doExtract is the main function for the 'extract' command.
// It initializes the logger, sets up the virtual file system (VFS), loads extension managers,
// and extracts a specific version of an OCFL object to a target folder.
func doExtract(cmd *cobra.Command, args []string) {
	rootPath := args[0]
	destPath := args[1]

	// Update configuration based on flags
	doExtractConf(cmd)

	oPath := conf.Extract.ObjectPath
	oID := conf.Extract.ObjectID
	if oPath != "" && oID != "" {
		cmd.Help()
		cobra.CheckErr(errors.New("do not use object-path AND object-id at the same time"))
		return
	}

	logger.Info().Msgf("extracting '%s'", rootPath)

	rootPath = writefs.RealPath(vfs, rootPath)
	destPath = writefs.RealPath(vfs, destPath)

	// Prepare source and destination filesystems
	ocflFS, err := writefs.Sub(vfs, rootPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", rootPath)
		return
	}
	destFS, err := writefs.SubCreate(vfs, destPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", destPath)
		return
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem: %v", destFS)
		}
	}()

	// Setup extension managers for storage root and object
	_, storageRootExtensionFactory, err := SetupExtensionManager[storageroot.ExtensionManager](
		cmd,
		logger,
		nil,
	)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return
	}

	objectExtensionManager, objectExtensionFactory, err := SetupExtensionManager[object.ExtensionManager](
		cmd,
		logger,
		firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)),
	)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Load storage root in read-only mode
	sr, err := LoadStorageRootRO(ctx, ocflFS, storageRootExtensionFactory, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return
	}

	dirs, err := fs.ReadDir(destFS, ".")
	if err != nil {
		logger.Error().Err(err).Msgf("cannot read target folder '%v'", destFS)
		return
	}
	if len(dirs) > 0 {
		fmt.Printf("target folder '%s' is not empty\n", destFS)
		logger.Debug().Msgf("target folder '%s' is not empty", destFS)
		return
	}
	if conf.Extract.ObjectID != "" {
		conf.Extract.ObjectPath, err = sr.IdToFolder(conf.Extract.ObjectID)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get object-path for '%s'", conf.Extract.ObjectID)
			return
		}
	}

	destAppendFS, ok := destFS.(appendfs.FS)
	if !ok {
		logger.Error().Err(err).Msgf("filesystem for '%s' is not writeable", destFS)
		return
	}

	// Perform the extraction
	if err := functions.Extract(
		context.Background(),
		sr.GetReadFS(),
		destAppendFS,
		conf.Extract.ObjectPath,
		inventorytypes.NewVersionNumber().WithString(conf.Extract.Version),
		conf.Extract.Manifest,
		conf.Extract.Area,
		objectExtensionFactory,
		logger,
	); err != nil {
		fmt.Printf("cannot extract storage root: %v\n", err)
		logger.Error().Err(err).Msg("cannot extract storage root")
		return
	}
	fmt.Printf("extraction done without errors\n")
	_ = showStatus(logger)
}
