package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/initocfl"
	"github.com/spf13/cobra"
)

var addCmd = &cobra.Command{
	Use:     "add [path to ocfl structure]",
	Aliases: []string{},
	Short:   "adds new object to existing ocfl structure",
	Long:    "opens an existing ocfl structure and adds a new object. if an object with the given id already exists, an error is produced",
	Example: "gocfl add ./archive.zip /tmp/testdata -u 'Jane Doe' -a 'mailto:user@domain' -m 'initial add' -object-id 'id:abc123'",
	Args:    cobra.MinimumNArgs(2),
	Run:     doAdd,
}

// initAdd initializes the gocfl add command
func initAdd() {
	addCmd.Flags().StringVarP(&flagObjectID, "object-id", "i", "", "object id to update (required)")
	addCmd.MarkFlagRequired("object-id")
	addCmd.Flags().String("default-object-extensions", "", "folder with initial extension configurations for new OCFL objects")
	addCmd.Flags().StringP("message", "m", "", "message for new object version (required)")
	addCmd.Flags().StringP("user-name", "u", "", "user name for new object version (required)")
	addCmd.Flags().StringP("user-address", "a", "", "user address for new object version (required)")
	addCmd.Flags().StringP("fixity", "f", "", "comma separated list of digest algorithms for fixity")
	addCmd.Flags().StringP("digest", "d", "", "digest to use for ocfl checksum")
	addCmd.Flags().Bool("deduplicate", false, "force deduplication (slower)")
	addCmd.Flags().Bool("no-compress", false, "do not compress data in zip file")
}

// doAddConf updates the configuration based on the command line flags for the 'add' command.
func doAddConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "fixity"); str != "" {
		parts := strings.Split(str, ",")
		for _, part := range parts {
			conf.Add.Fixity = append(conf.Add.Fixity, part)
		}
	}
	for _, alg := range conf.Add.Fixity {
		alg = strings.TrimSpace(strings.ToLower(alg))
		if alg == "" {
			continue
		}
		if _, err := checksum.GetHash(checksum.DigestAlgorithm(alg)); err != nil {
			_ = cmd.Help()
			cobra.CheckErr(errors.Errorf("invalid fixity '%s' for flag 'fixity' or 'Add.Fixity' config file entry", conf.Add.Fixity))
		}
	}

	if str := getFlagString(cmd, "user-name"); str != "" {
		conf.Add.User.Name = str
	}
	if str := getFlagString(cmd, "user-address"); str != "" {
		conf.Add.User.Address = str
	}
	if str := getFlagString(cmd, "message"); str != "" {
		conf.Add.Message = str
	}
	if str := getFlagString(cmd, "default-object-extensions"); str != "" {
		conf.Add.ObjectExtensionFolder = str
	}
	if b, ok := getFlagBool(cmd, "deduplicate"); b {
		if ok {
			conf.Add.Deduplicate = b
		}
	}
	if b, ok := getFlagBool(cmd, "no-compress"); b {
		if ok {
			conf.Add.NoCompress = b
		}
	}

	if str := getFlagString(cmd, "digest"); str != "" {
		conf.Add.Digest = checksum.DigestAlgorithm(str)
	}
	if conf.Add.Digest == "" {
		conf.Add.Digest = checksum.DigestSHA512
	}
	if _, err := checksum.GetHash(conf.Add.Digest); err != nil {
		_ = cmd.Help()
		cobra.CheckErr(errors.Errorf("invalid digest '%s' for flag 'digest' or 'Init.DigestAlgorithm' config file entry", conf.Add.Digest))
	}

}

// doAdd is the main function for the 'add' command.
// It initializes the logger, sets up the virtual file system (VFS), loads extension managers,
// and adds a new object to an existing OCFL structure.
func doAdd(cmd *cobra.Command, args []string) {
	if err := cmd.ValidateRequiredFlags(); err != nil {
		cobra.CheckErr(err)
		return
	}

	ocflPath := args[0]
	srcPath := args[1]

	/*
		if !slices.Contains([]string{"DEBUG", "ERROR", "WARNING", "INFO", "CRITICAL"}, conf.Log.Level) {
			_ = cmd.Help()
			cobra.CheckErr(errors.Errorf("invalid log level '%s' for flag 'log-level' or 'LogLevel' config file entry", persistentFlagLoglevel))
		}
	*/

	// Update configuration based on flags
	doAddConf(cmd)

	var addr string
	var localCache bool

	fmt.Printf("opening '%s'\n", ocflPath)
	logger.Info().Msgf("opening '%s'", ocflPath)

	var fixityAlgs = []checksum.DigestAlgorithm{}
	for _, alg := range conf.Add.Fixity {
		alg = strings.TrimSpace(strings.ToLower(alg))
		if alg == "" {
			continue
		}
		fixityAlgs = append(fixityAlgs, checksum.DigestAlgorithm(alg))
	}

	ocflPath = writefs.RealPath(vfs, ocflPath)
	srcPath = writefs.RealPath(vfs, srcPath)

	if _, err := fs.Stat(vfs, srcPath); err != nil {
		logger.Fatal().Err(err).Msgf("cannot stat '%s'", srcPath)
	}

	// Prepare source and destination filesystems
	sourceFS, err := writefs.Sub(vfs, srcPath)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get filesystem for '%s'", srcPath)
	}
	_destFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Fatal().Msgf("cannot get filesystem for '%s'", ocflPath)
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Fatal().Msgf("filesystem for '%s' is not writeable", ocflPath)
	}
	var doNotClose = false
	defer func() {
		if doNotClose {
			logger.Fatal().Msgf("filesystem '%s' not closed", destFS)
		} else {
			if err := writefs.Close(destFS); err != nil {
				logger.Fatal().Msgf("error closing filesystem '%s'", destFS)
			}
		}
	}()

	area := conf.DefaultArea
	if area == "" {
		area = "content"
	}
	var areaPaths = map[string]fs.FS{}
	for i := 2; i < len(args); i++ {
		matches := areaPathRegexp.FindStringSubmatch(args[i])
		if matches == nil {
			logger.Error().Msgf("no area given in areapath '%s'", args[i])
			continue
		}
		areaPaths[matches[1]], err = writefs.Sub(vfs, matches[2])
		if err != nil {
			doNotClose = true
			logger.Fatal().Msgf("cannot get filesystem for '%s'", args[i])
		}
	}

	ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
	ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
	ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
	ext_NNNN_metafile.Init(vfs, logger)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot get extension params")
	}

	logger.Debug().Msgf("initializing ExtensionFactory")

	// Load storage root
	storageRoot, err := initocfl.LoadStorageRoot(ctx, destFS, extensionParams, nil, logger)
	if err != nil {
		doNotClose = true
		logger.Fatal().Err(err).Msg("cannot open storage root")
	}
	defer storageRoot.Close()
	if storageRoot.GetDigest() == "" {
		storageRoot.SetDigest(checksum.DigestAlgorithm(conf.Add.Digest))
	} else {
		if storageRoot.GetDigest() != conf.Add.Digest {
			doNotClose = true
			logger.Fatal().Msgf("storageroot already uses digest '%s' not '%s'", storageRoot.GetDigest(), conf.Add.Digest)
		}
	}

	// Add the object to the storage root
	_, err = addObjectByPath(
		ctx,
		storageRoot,
		fixityAlgs,
		extensionParams,
		conf.Add.Deduplicate,
		flagObjectID,
		conf.Add.User.Name,
		conf.Add.User.Address,
		conf.Add.Message,
		sourceFS,
		firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)),
		area,
		areaPaths,
		false,
		logger)
	if err != nil {
		doNotClose = true
		logger.Fatal().Err(err).Msgf("error adding content to storageroot filesystem '%s'", destFS)
	}
	_ = showStatus(logger)

}
