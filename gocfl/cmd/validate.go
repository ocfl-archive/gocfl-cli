package cmd

import (
	"io/fs"
	"os"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	defaultextensions_storageroot "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
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
	RunE:    doValidate,
}

func initValidate() {
	validateCmd.Flags().StringP("object-path", "o", "", "validate only the object at the specified path in storage root")
	validateCmd.Flags().StringP("object-id", "i", "", "validate only the object with the specified id in storage root")
}

// doValidateConf updates the configuration based on the command line flags.
func doValidateConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "object-path"); str != "" {
		if err := conf.Validate.ObjectPath.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid object-path '%s' for flag 'object-path' or 'Validate.ObjectPath' config file entry", str)
			return errors.Wrapf(err, "invalid object-path '%s'", str)
		}
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.Validate.ObjectID = str
	}
	return nil
}

// doValidate is the main function for the 'validate' command.
// It initializes the logger, loads extension managers for storage root and objects,
// sets up the virtual file system (VFS), and performs the actual validation.
func doValidate(cmd *cobra.Command, args []string) error {
	ocflPath := args[0]

	// Update configuration based on flags
	if err := doValidateConf(cmd); err != nil {
		return err
	}

	logger.Info().Msgf("validating '%s'", ocflPath)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return errors.Wrap(err, "cannot get extension params")
	}

	// Load extension manager for storage root
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

	// Load extension manager for objects
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

	ocflPath = writefs.RealPath(vfs, ocflPath)

	var destFS fs.FS
	if strings.HasSuffix(strings.ToLower(ocflPath), ".zip") {
		var err error
		destFS, err = zipfs.NewFSFile(vfs, ocflPath, logger.Logger())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip filesystem for '%s'", ocflPath)
			return errors.Wrapf(err, "cannot open zip filesystem for '%s'", ocflPath)
		}
	} else {
		// Prepare access to the OCFL directory
		var err error
		destFS, err = writefs.Sub(vfs, ocflPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
			return errors.Wrapf(err, "cannot get filesystem for '%s'", ocflPath)
		}
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem for '%s'", ocflPath)
		}
	}()

	// Load storage root in read-only mode
	sr, err := ocfl.LoadStorageRoot(ctx, destFS, nil, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storageroot")
		return errors.Wrap(err, "cannot load storageroot")
	}
	defer sr.Close()
	objectID := conf.Validate.ObjectID
	objectPath := conf.Validate.ObjectPath.String()
	if objectID != "" && objectPath != "" {
		logger.Error().Msg("do not use object-path AND object-id at the same time")
		return errors.New("do not use object-path AND object-id at the same time")
	}

	// If no specific object ID or path was specified, validate the entire storage root
	if objectID == "" && objectPath == "" {
		if err := sr.Check(); err != nil {
			logger.Error().Err(err).Msg("ocfl not valid")
			return errors.Wrap(err, "ocfl not valid")
		}
	} else {
		// Validation of a single object
		if objectID != "" {
			// Resolve object ID to path
			objectPath, err = sr.IdToFolder(objectID)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot get object-path for '%s'", objectID)
				return errors.Wrapf(err, "cannot get object-path for '%s'", objectID)
			}
		}
		// Create sub-filesystem for the object
		objFsys, err := writefs.Sub(sr.GetReadFS(), objectPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open filesystem for '%s'", objectPath)
			return errors.Wrapf(err, "cannot open filesystem for '%s'", objectPath)
		}
		// Load object
		obj, err := ocfl.LoadObject(ctx, objFsys, nil, logger)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open object for '%s'", objectPath)
			return errors.Wrapf(err, "cannot open object for '%s'", objectPath)
		}
		defer obj.Close()
		// Get checker for the object and execute validation
		checker := obj.GetValidator()
		defer checker.Close()
		if err := checker.Validate(); err != nil {
			logger.Error().Err(err).Msgf("ocfl object '%s' not valid", objectPath)
			return errors.Wrapf(err, "ocfl object '%s' not valid", objectPath)
		}

	}
	if showStatus(logger) {
		return errors.New("validation failed")
	}
	return nil
}
