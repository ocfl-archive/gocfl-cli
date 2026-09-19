package cmd

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var extractMetaCmd = &cobra.Command{
	Use:     "extractmeta [path to ocfl structure]",
	Aliases: []string{},
	Short:   "extract metadata from ocfl structure",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl extractmeta ./archive.zip --output-json ./archive_meta.json",
	Args:    cobra.ExactArgs(1),
	RunE:    doExtractMeta,
}

func initExtractMeta() {
	extractMetaCmd.Flags().StringP("object-path", "p", "", "object path to extract")
	extractMetaCmd.Flags().StringP("object-id", "i", "", "object id to extract")
	extractMetaCmd.Flags().String("version", "latest", "version to extract")
	extractMetaCmd.Flags().String("format", "json", "output format (json, human)")
	extractMetaCmd.Flags().String("output", "", "output file (default stdout)")
	extractMetaCmd.Flags().Bool("obfuscate", false, "obfuscate metadata")
}

// doExtractMetaConf updates the configuration based on the command line flags for the 'extractmeta' command.
func doExtractMetaConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "object-path"); str != "" {
		if err := conf.ExtractMeta.ObjectPath.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid object-path '%s' for flag 'object-path' or 'ExtractMeta.ObjectPath' config file entry", str)
			return errors.Wrapf(err, "invalid object-path '%s'", str)
		}
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.ExtractMeta.ObjectID = str
	}
	if str := getFlagString(cmd, "version"); str != "" {
		conf.ExtractMeta.Version = str
	}
	if conf.ExtractMeta.Version == "" {
		conf.ExtractMeta.Version = "latest"
	}
	if str := getFlagString(cmd, "format"); str != "" {
		conf.ExtractMeta.Format = str
	}
	if str := getFlagString(cmd, "output"); str != "" {
		conf.ExtractMeta.Output = str
	}
	if b, ok := getFlagBool(cmd, "obfuscate"); ok {
		conf.ExtractMeta.Obfuscate = b
	}
	return nil
}

// doExtractMeta is the main function for the 'extractmeta' command.
// It extracts metadata from an OCFL object and outputs it in JSON format.
func doExtractMeta(cmd *cobra.Command, args []string) error {
	ocflPath := args[0]

	// Update configuration based on flags
	if err := doExtractMetaConf(cmd); err != nil {
		return err
	}

	oPath := conf.ExtractMeta.ObjectPath.String()
	oID := conf.ExtractMeta.ObjectID
	if oPath != "" && oID != "" {
		_ = cmd.Help()
		return errors.New("do not use object-path AND object-id at the same time")
	}
	if oPath == "" && oID == "" {
		_ = cmd.Help()
		return errors.New("must specify either object-id or object-path")
	}
	format := strings.ToLower(conf.ExtractMeta.Format)
	if format != "json" && format != "human" {
		_ = cmd.Help()
		return errors.Errorf("invalid format '%s' for flag 'format' or 'Format' config file entry", format)
	}
	output := conf.ExtractMeta.Output

	ocflPath = writefs.RealPath(vfs, ocflPath)
	logger.Info().Msgf("vfs created : %v", vfs)

	logger.Info().Msgf("extracting metadata from '%s'", ocflPath)

	var ocflFS fs.FS
	if strings.ToLower(path.Ext(ocflPath)) == ".zip" {
		zipFS, err := zipfs.NewFSFile(vfs, ocflPath, logger.Logger())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip filesystem at '%s'", ocflPath)
			return errors.Wrapf(err, "cannot open zip filesystem at '%s'", ocflPath)
		}
		defer zipFS.Close()
		ocflFS = zipFS
	} else {
		var err error
		ocflFS, err = writefs.Sub(vfs, ocflPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open ocfl filesystem at '%s'", ocflPath)
			return errors.Wrapf(err, "cannot open ocfl filesystem at '%s'", ocflPath)
		}
	}
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
	logger.WithVersion(sr.GetOCFLVersion())
	if oID != "" {
		var err error
		oPath, err = sr.IdToFolder(oID)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get id folder for '%s'", oID)
			return errors.Wrapf(err, "cannot get id folder for '%s'", oID)
		}
	}

	objPathFS, err := fs.Sub(sr.GetReadFS(), oPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get subfs for '%s'", oPath)
		return errors.Wrapf(err, "cannot get subfs for '%s'", oPath)
	}
	obj, err := ocfl.LoadObject(ctx, objPathFS, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load object")
		return errors.Wrap(err, "cannot load object")
	}
	defer obj.Close()
	extractor := obj.GetExtractor()
	defer extractor.Close()
	metadata, err := extractor.GetMetadata()
	if err != nil {
		fmt.Printf("cannot extract metadata from storage root: %v\n", err)
		logger.Error().Err(err).Msg("cannot extract metadata from storage root")
		return errors.Wrap(err, "cannot extract metadata from storage root")
	}
	if conf.ExtractMeta.Obfuscate {
		if err := metadata.Obfuscate(); err != nil {
			fmt.Printf("cannot obfuscate metadata: %v\n", err)
			logger.Error().Err(err).Msg("cannot obfuscate metadata")
			return errors.Wrap(err, "cannot obfuscate metadata")
		}
	}

	var outputBytes []byte
	if format == "human" {
		outputBytes = []byte(metadata.String())
	} else {
		outputBytes, err = json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			fmt.Printf("cannot marshal metadata\n")
			logger.Error().Err(err).Msg("cannot marshal metadata")
			return errors.Wrap(err, "cannot marshal metadata")
		}
	}

	if output != "" {
		if err := os.WriteFile(output, outputBytes, 0644); err != nil {
			fmt.Printf("cannot write to file '%s'\n", output)
			logger.Error().Err(err).Msgf("cannot write to file '%s'", output)
			return errors.Wrapf(err, "cannot write to file '%s'", output)
		}
	} else {
		if _, err := os.Stdout.Write(outputBytes); err != nil {
			fmt.Printf("cannot write to standard output\n")
			logger.Error().Err(err).Msg("cannot write to standard output")
			return errors.Wrap(err, "cannot write to standard output")
		}
		fmt.Print("\n")
	}
	fmt.Printf("metadata extraction done without errors\n")
	if showStatus(logger) {
		return errors.New("extractmeta failed")
	}
	return nil
}
