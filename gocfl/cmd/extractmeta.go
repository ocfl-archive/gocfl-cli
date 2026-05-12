package cmd

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/filesystem/v4/pkg/vfsrw"
	"github.com/je4/filesystem/v4/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl-cli/internal"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/functions"
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
	Run:     doExtractMeta,
}

func initExtractMeta() {
	extractMetaCmd.Flags().StringP("object-path", "p", "", "object path to extract")
	extractMetaCmd.Flags().StringP("object-id", "i", "", "object id to extract")
	extractMetaCmd.Flags().String("version", "latest", "version to extract")
	extractMetaCmd.Flags().String("format", "json", "output format (json)")
	extractMetaCmd.Flags().String("output", "", "output file (default stdout)")
	extractMetaCmd.Flags().Bool("obfuscate", false, "obfuscate metadata")
}

// doExtractMetaConf updates the configuration based on the command line flags for the 'extractmeta' command.
func doExtractMetaConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "object-path"); str != "" {
		conf.ExtractMeta.ObjectPath = str
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
}

// doExtractMeta is the main function for the 'extractmeta' command.
// It extracts metadata from an OCFL object and outputs it in JSON format.
func doExtractMeta(cmd *cobra.Command, args []string) {
	ocflPath := args[0]

	// Update configuration based on flags
	doExtractMetaConf(cmd)

	oPath := conf.ExtractMeta.ObjectPath
	oID := conf.ExtractMeta.ObjectID
	if oPath != "" && oID != "" {
		cmd.Help()
		cobra.CheckErr(errors.New("do not use object-path AND object-id at the same time"))
		return
	}
	if oPath == "" && oID == "" {
		cmd.Help()
		cobra.CheckErr(errors.New("must specify either object-id or object-path"))
		return
	}
	format := strings.ToLower(conf.ExtractMeta.Format)
	if format != "json" {
		cmd.Help()
		cobra.CheckErr(errors.Errorf("invalid format '%s' for flag 'format' or 'Format' config file entry", format))
		return
	}
	output := conf.ExtractMeta.Output

	ocflPath = writefs.RealPath(vfs, ocflPath)
	if err := vfsrw.AddLocal(vfs, &vfsrw.ZipAsFolder{
		Enabled:   true,
		Digests:   nil,
		CacheSize: 2,
		Compress:  false,
		ReadOnly:  true,
		AES:       nil,
	}); err != nil {
		logger.Error().Err(err).Msg("cannot add local filesystem to vfs")
	}
	vfs.AddFS("internal", nil, internal.InternalFS)

	/*
		ocflPath, err = path2vfs(ocflPath)
		if err != nil {
			logger.Error().Err(err).Msg("cannot create ocfl path")
			return
		}
	*/

	logger.Info().Msgf("vfs created : %v", vfs)

	logger.Info().Msgf("extracting metadata from '%s'", ocflPath)

	/*
		fsFactory, err := initializeFSFactory(nil, nil, nil, true, true, logger)
		if err != nil {
			logger.Error().Err(err).Msg("cannot create filesystem factory")
			return
		}
		ocflFS, err := fsFactory.Get(ocflPath, true)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
			return
		}
		defer func() {
			if err := writefs.Close(ocflFS); err != nil {
				logger.Error().Err(err).Msgf("cannot close filesystem for '%s'", ocflFS)
			}
		}()
	*/
	// Prepare access to the OCFL directory
	ocflFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot open ocfl filesystem at '%s'", ocflPath)
		return
	}
	// Setup extension factories for storage root and object
	storageRootExtensionFactory, err := setupExtensionFactory[storageroot.ExtensionManager](cmd, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Factory fail")
		return
	}
	objectExtensionFactory, err := setupExtensionFactory[object.ExtensionManager](cmd, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Factory fail")
		return
	}

	// Load object extension manager
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

	// Load storage root in read-only mode
	sr, err := LoadStorageRootRO(ctx, ocflFS, storageRootExtensionFactory, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return
	}
	logger.WithVersion(sr.GetOCFLVersion())
	if oID != "" {
		oPath, err = sr.IdToFolder(oID)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get id folder for '%s'", oID)
			return
		}
	}

	metadata, err := functions.ExtractMeta(ctx, sr.GetReadFS(), oPath, objectExtensionFactory, logger)
	if err != nil {
		fmt.Printf("cannot extract metadata from storage root: %v\n", err)
		logger.Error().Err(err).Msg("cannot extract metadata from storage root")
		return
	}
	if conf.ExtractMeta.Obfuscate {
		if err := metadata.Obfuscate(); err != nil {
			fmt.Printf("cannot obfuscate metadata: %v\n", err)
			logger.Error().Err(err).Msg("cannot obfuscate metadata")
			return
		}
	}

	jsonBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		fmt.Printf("cannot marshal metadata")
		logger.Error().Err(err).Msg("cannot marshal metadata")
		return
	}
	if output != "" {
		if err := os.WriteFile(output, jsonBytes, 0644); err != nil {
			fmt.Printf("cannot write json to file")
			logger.Error().Err(err).Msgf("cannot write json to file '%s'", output)
			return
		}
	} else {
		if _, err := os.Stdout.Write(jsonBytes); err != nil {
			fmt.Printf("cannot write json to file")
			logger.Error().Err(err).Msg("cannot write json to file standard output")
			return
		}
		fmt.Print("\n")
	}
	fmt.Printf("metadata extraction done without errors\n")
	_ = showStatus(logger)
}
