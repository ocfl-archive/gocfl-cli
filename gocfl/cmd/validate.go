package cmd

import (
	"context"
	"io/fs"
	"log"
	"os"

	"github.com/je4/filesystem/v4/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/functions"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
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

	// Initialize context and logger
	ctx := context.TODO()
	logger, closers, err := setupLogger(ctx, version.Default)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()

	// Start timer for validation duration
	t := startTimer()
	defer func() { logger.Info().Msgf("Duration: %s", t.String()) }()

	// Update configuration based on flags
	doValidateConf(cmd)

	logger.Info().Msgf("validating '%s'", ocflPath)

	// Load extension factory and manager for storage root
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

	// Load extension factory and manager for objects
	objectExtensionFactory, err := setupExtensionFactory[object.ExtensionManager](cmd, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Factory fail")
		return
	}

	objectExtensionManager, err := LoadExtensionManager(
		objectExtensionFactory,
		firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)),
	)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root extension")
		return
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Initialize virtual file system
	vfs, err := setupVFS(logger)
	if err != nil {
		logger.Error().Err(err).Msg("VFS fail")
		return
	}
	defer vfs.Close()
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
	sr, err := LoadStorageRootRO(ctx, destFS, storageRootExtensionFactory, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storageroot")
		return
	}
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
		objFsys, err := fs.Sub(sr.GetReadFS(), objectPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open filesystem for '%s'", objectPath)
			return
		}
		// Load object
		obj, err := functions.LoadObject(ctx, objFsys, objectExtensionFactory, logger)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open object for '%s'", objectPath)
			return
		}
		// Get checker for the object and execute validation
		checker := obj.GetChecker(objFsys)
		if err := checker.Check(); err != nil {
			logger.Error().Err(err).Msgf("ocfl object '%s' not valid", objectPath)
			return
		}

	}
	_ = showStatus(logger)
}
