package cmd

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"
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
	ocfllogger "github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/spf13/cobra"
)

type companionChecksum struct {
	alg      string
	path     string
	expected string
}

func findCompanionChecksums(vfs fs.FS, zipPath string) ([]companionChecksum, error) {
	var checksums []companionChecksum
	for _, alg := range []string{"sha512", "sha256"} {
		csPath := zipPath + "." + alg
		data, err := fs.ReadFile(vfs, csPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err) {
				continue
			}
			if _, statErr := fs.Stat(vfs, csPath); statErr != nil {
				continue
			}
			return nil, errors.Wrapf(err, "cannot read companion checksum file '%s'", csPath)
		}
		fields := strings.Fields(string(data))
		if len(fields) == 0 {
			return nil, errors.Errorf("empty companion checksum file '%s'", csPath)
		}
		checksums = append(checksums, companionChecksum{
			alg:      alg,
			path:     csPath,
			expected: fields[0],
		})
	}
	return checksums, nil
}

func verifyZipChecksumsAsync(vfs fs.FS, zipPath string, checksums []companionChecksum, logger ocfllogger.OCFLLogger) <-chan error {
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		if len(checksums) == 0 {
			return
		}

		f, err := vfs.Open(zipPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip file '%s' for checksum calculation", zipPath)
			errChan <- errors.Wrapf(err, "cannot open zip file '%s'", zipPath)
			return
		}
		defer f.Close()

		var writers []io.Writer
		hashers := make(map[string]hash.Hash)
		for _, cs := range checksums {
			switch strings.ToLower(cs.alg) {
			case "sha256":
				h := sha256.New()
				hashers["sha256"] = h
				writers = append(writers, h)
			case "sha512":
				h := sha512.New()
				hashers["sha512"] = h
				writers = append(writers, h)
			default:
				err := errors.Errorf("unsupported checksum algorithm '%s'", cs.alg)
				logger.Error().Err(err).Msgf("unsupported checksum algorithm '%s'", cs.alg)
				errChan <- err
				return
			}
		}

		mw := io.MultiWriter(writers...)
		if _, err := io.Copy(mw, f); err != nil {
			logger.Error().Err(err).Msgf("error reading zip file '%s' for checksum calculation", zipPath)
			errChan <- errors.Wrapf(err, "error reading zip file '%s'", zipPath)
			return
		}

		var errs []error
		for _, cs := range checksums {
			h, ok := hashers[strings.ToLower(cs.alg)]
			if !ok {
				continue
			}
			computed := hex.EncodeToString(h.Sum(nil))
			if !strings.EqualFold(computed, cs.expected) {
				mismatchErr := errors.Errorf("%s checksum mismatch for '%s': expected %s, got %s", cs.alg, zipPath, cs.expected, computed)
				logger.Error().Err(mismatchErr).Msgf("invalid %s checksum for '%s'", cs.alg, zipPath)
				errs = append(errs, mismatchErr)
			} else {
				logger.Info().Msgf("valid %s checksum for '%s'", cs.alg, zipPath)
			}
		}

		if len(errs) > 0 {
			errChan <- errors.Combine(errs...)
		}
	}()
	return errChan
}

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

	var checksumErrChan <-chan error
	var destFS fs.FS
	if strings.HasSuffix(strings.ToLower(ocflPath), ".zip") {
		companionChecksums, err := findCompanionChecksums(vfs, ocflPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot find companion checksums for '%s'", ocflPath)
			return errors.Wrapf(err, "cannot find companion checksums for '%s'", ocflPath)
		}
		if len(companionChecksums) > 0 {
			checksumErrChan = verifyZipChecksumsAsync(vfs, ocflPath, companionChecksums, logger)
		}

		destFS, err = zipfs.NewFSFile(vfs, ocflPath, logger.Logger())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip filesystem for '%s'", ocflPath)
			if checksumErrChan != nil {
				<-checksumErrChan
			}
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
		if checksumErrChan != nil {
			<-checksumErrChan
		}
		return errors.Wrap(err, "cannot load storageroot")
	}
	defer sr.Close()
	objectID := conf.Validate.ObjectID
	objectPath := conf.Validate.ObjectPath.String()
	if objectID != "" && objectPath != "" {
		logger.Error().Msg("do not use object-path AND object-id at the same time")
		if checksumErrChan != nil {
			<-checksumErrChan
		}
		return errors.New("do not use object-path AND object-id at the same time")
	}

	var validationErr error
	// If no specific object ID or path was specified, validate the entire storage root
	if objectID == "" && objectPath == "" {
		if err := sr.Check(); err != nil {
			logger.Error().Err(err).Msg("ocfl not valid")
			validationErr = errors.Wrap(err, "ocfl not valid")
		}
	} else {
		// Validation of a single object
		if objectID != "" {
			// Resolve object ID to path
			objectPath, err = sr.IdToFolder(objectID)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot get object-path for '%s'", objectID)
				validationErr = errors.Wrapf(err, "cannot get object-path for '%s'", objectID)
			}
		}
		if validationErr == nil {
			// Create sub-filesystem for the object
			objFsys, err := writefs.Sub(sr.GetReadFS(), objectPath)
			if err != nil {
				logger.Error().Err(err).Msgf("cannot open filesystem for '%s'", objectPath)
				validationErr = errors.Wrapf(err, "cannot open filesystem for '%s'", objectPath)
			} else {
				// Load object
				obj, err := ocfl.LoadObject(ctx, objFsys, nil, logger)
				if err != nil {
					logger.Error().Err(err).Msgf("cannot open object for '%s'", objectPath)
					validationErr = errors.Wrapf(err, "cannot open object for '%s'", objectPath)
				} else {
					defer obj.Close()
					// Get checker for the object and execute validation
					checker := obj.GetValidator()
					defer checker.Close()
					if err := checker.Validate(); err != nil {
						logger.Error().Err(err).Msgf("ocfl object '%s' not valid", objectPath)
						validationErr = errors.Wrapf(err, "ocfl object '%s' not valid", objectPath)
					}
				}
			}
		}
	}

	var checksumErr error
	if checksumErrChan != nil {
		checksumErr = <-checksumErrChan
	}

	if validationErr != nil {
		return validationErr
	}
	if checksumErr != nil {
		return checksumErr
	}

	if showStatus(logger) {
		return errors.New("validation failed")
	}
	return nil
}
