package cmd

import (
	"io/fs"
	"os"

	"github.com/ocfl-archive/filesystem/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:     "validate [path to ocfl structure]",
	Aliases: []string{"check"},
	Short:   "validates an ocfl structure",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl validate ./archive.zip",
	Args:    cobra.ExactArgs(1),
	Run:     doValidate,
}

func initValidate() {
	validateCmd.Flags().StringP("object-path", "o", "", "validate only the object at the specified path in storage root")
	validateCmd.Flags().StringP("object-id", "i", "", "validate only the object with the specified id in storage root")
}

// doValidateConf updates the configuration based on the command line flags.
func doValidateConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "object-path"); str != "" {
		conf.Validate.ObjectPath = str
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.Validate.ObjectID = str
	}
}

// doValidate is the main function for the 'validate' command.
// It initializes the logger, loads extension managers for storage root and objects,
// sets up the virtual file system (VFS), and performs the actual validation.
func doValidate(cmd *cobra.Command, args []string) {
	ocflPath := args[0]

	// Update configuration based on flags
	doValidateConf(cmd)

	logger.Info().Msgf("validating '%s'", ocflPath)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return
	}

	// Load extension manager for storage root
	storageRootExtensionManager, _, err := initocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, firstOrSecond(conf.Init.StorageRootExtensionFolder == "", (fs.FS)(defaultextensions_storageroot.DefaultStorageRootExtensionFS), os.DirFS(conf.Init.StorageRootExtensionFolder)), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return
	}
	defer func() {
		if err := storageRootExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Load extension manager for objects
	objectExtensionManager, _, err := initocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	ocflPath = writefs.RealPath(vfs, ocflPath)

	// Prepare access to the OCFL directory
	destFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
		return
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem for '%s'", destFS)
		}
	}()

	// Load storage root in read-only mode
	sr, err := initocfl.LoadStorageRoot(ctx, destFS, nil, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storageroot")
		return
	}
	defer sr.Close()
	objectID := conf.Validate.ObjectID
	objectPath := conf.Validate.ObjectPath
	if objectID != "" && objectPath != "" {
		logger.Error().Msg("do not use object-path AND object-id at the same time")
		return
	}

	// If no specific object ID or path was specified, validate the entire storage root
	if objectID == "" && objectPath == "" {
		if err := sr.Check(); err != nil {
			logger.Error().Err(err).Msg("ocfl not valid")
			return
		}
	} else {
		// Validation of a single object
		if objectID != "" {
			// Resolve object ID to path
			objectPath, err = sr.IdToFolder(objectID)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot get object-path for '%s'", objectID)
				return
			}
		}
		// Create sub-filesystem for the object
		objFsys, err := writefs.Sub(sr.GetReadFS(), objectPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open filesystem for '%s'", objectPath)
			return
		}
		// Load object
		obj, err := initocfl.LoadObject(ctx, objFsys, nil, logger)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open object for '%s'", objectPath)
			return
		}
		defer obj.Close()
		// Get checker for the object and execute validation
		checker := obj.GetChecker()
		if err := checker.Check(); err != nil {
			logger.Error().Err(err).Msgf("ocfl object '%s' not valid", objectPath)
			return
		}

	}
	_ = showStatus(logger)
}
