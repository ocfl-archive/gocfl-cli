package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
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
	RunE:    doExtract,
}

func initExtract() {
	extractCmd.Flags().StringP("object-path", "p", "", "object path to extract")
	extractCmd.Flags().StringP("object-id", "i", "", "object id to extract")
	extractCmd.Flags().Bool("with-manifest", false, "generate manifest file in object extraction folder")
	extractCmd.Flags().String("version", "", "version to extract")
	extractCmd.Flags().String("area", "content", "data area to extract")
}

// doExtractConf updates the configuration based on the command line flags for the 'extract' command.
func doExtractConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "object-path"); str != "" {
		if err := conf.Extract.ObjectPath.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid object-path '%s' for flag 'object-path' or 'Extract.ObjectPath' config file entry", str)
			return errors.Wrapf(err, "invalid object-path '%s'", str)
		}
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.Extract.ObjectID = str
	}
	if b, ok := getFlagBool(cmd, "with-manifest"); ok {
		conf.Extract.Manifest = b
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
	return nil
}

// doExtract is the main function for the 'extract' command.
// It initializes the logger, sets up the virtual file system (VFS), loads extension managers,
// and extracts a specific version of an OCFL object to a target folder.
func doExtract(cmd *cobra.Command, args []string) error {
	rootPath := args[0]
	destPath := args[1]

	// Update configuration based on flags
	if err := doExtractConf(cmd); err != nil {
		return err
	}

	oPath := conf.Extract.ObjectPath.String()
	oID := conf.Extract.ObjectID
	if oPath != "" && oID != "" {
		_ = cmd.Help()
		return errors.New("do not use object-path AND object-id at the same time")
	}

	logger.Info().Msgf("extracting '%s'", rootPath)

	rootPath = writefs.RealPath(vfs, rootPath)
	destPath = writefs.RealPath(vfs, destPath)

	// Prepare source and destination filesystems
	var ocflFS fs.FS
	if strings.ToLower(path.Ext(rootPath)) == ".zip" {
		zipFS, err := zipfs.NewFSFile(vfs, rootPath, logger.Logger())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip filesystem at '%s'", rootPath)
			return errors.Wrapf(err, "cannot open zip filesystem at '%s'", rootPath)
		}
		defer zipFS.Close()
		ocflFS = zipFS
	} else {
		var err error
		ocflFS, err = writefs.Sub(vfs, rootPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open ocfl filesystem at '%s'", rootPath)
			return errors.Wrapf(err, "cannot open ocfl filesystem at '%s'", rootPath)
		}
	}
	destFS, err := writefs.SubCreate(vfs, destPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", destPath)
		return errors.Wrapf(err, "cannot get filesystem for '%s'", destPath)
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem: %v", destFS)
		}
	}()

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return errors.Wrap(err, "cannot get extension params")
	}

	// Setup extension managers for storage root and object
	_, _, err = ocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return errors.Wrap(err, "cannot setup storage root extension manager")
	}

	objectExtensionManager, _, err := ocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder.String())), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return errors.Wrap(err, "cannot setup object extension manager")
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate object extension manager")
		}
	}()

	// Load storage root in read-only mode
	sr, err := ocfl.LoadStorageRoot(ctx, ocflFS, nil, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return errors.Wrap(err, "cannot load storage root")
	}
	defer sr.Close()

	dirs, err := fs.ReadDir(destFS, ".")
	if err != nil {
		logger.Error().Err(err).Msgf("cannot read target folder '%v'", destFS)
		return errors.Wrapf(err, "cannot read target folder '%v'", destFS)
	}
	if len(dirs) > 0 {
		fmt.Printf("target folder '%s' is not empty\n", destFS)
		logger.Debug().Msgf("target folder '%s' is not empty", destFS)
		return errors.Errorf("target folder '%s' is not empty", destPath)
	}
	if conf.Extract.ObjectID != "" {
		p, err := sr.IdToFolder(conf.Extract.ObjectID)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get object-path for '%s'", conf.Extract.ObjectID)
			return errors.Wrapf(err, "cannot get object-path for '%s'", conf.Extract.ObjectID)
		}
		if err := conf.Extract.ObjectPath.UnmarshalText([]byte(p)); err != nil {
			logger.Error().Err(err).Msgf("invalid object-path '%s' for flag 'object-path' or 'Extract.ObjectPath' config file entry", p)
			return errors.Wrapf(err, "invalid object-path '%s'", p)
		}
	}

	destAppendFS, ok := destFS.(appendfs.FS)
	if !ok {
		logger.Error().Msgf("filesystem for '%s' is not writeable", destFS)
		return errors.Errorf("filesystem for '%s' is not writeable", destPath)
	}

	var objFS fs.FS = ocflFS
	if conf.Extract.ObjectPath.String() != "" {
		objFS, err = writefs.Sub(sr.GetReadFS(), conf.Extract.ObjectPath.String())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get subfs for '%s'", conf.Extract.ObjectPath)
			return errors.Wrapf(err, "cannot get subfs for '%s'", conf.Extract.ObjectPath)
		}
	}

	// Perform the extraction
	obj, err := ocfl.LoadObject(context.Background(), objFS, extensionParams, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load object")
		return errors.Wrap(err, "cannot load object")
	}
	defer obj.Close()
	extractor := obj.GetExtractor()
	defer extractor.Close()
	if err := extractor.
		WithDestFS(destAppendFS).
		Extract(
			inventorytypes.NewVersionNumber().WithString(conf.Extract.Version),
			conf.Extract.Manifest,
			conf.Extract.Area,
		); err != nil {
		fmt.Printf("cannot extract storage root: %v\n", err)
		logger.Error().Err(err).Msg("cannot extract storage root")
		return errors.Wrap(err, "cannot extract storage root")
	}
	fmt.Printf("extraction done without errors\n")
	if showStatus(logger) {
		return errors.New("extract failed")
	}
	return nil
}
