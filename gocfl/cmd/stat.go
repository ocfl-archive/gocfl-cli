package cmd

import (
	"fmt"
	"os"
	"strings"

	"emperror.dev/emperror"
	"emperror.dev/errors"
	"github.com/je4/filesystem/v4/pkg/appendfs"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var statCmd = &cobra.Command{
	Use:     "stat [path to ocfl structure]",
	Aliases: []string{"info"},
	Short:   "statistics of an ocfl structure",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl stat ./archive.zip",
	Args:    cobra.MinimumNArgs(1),
	Run:     doStat,
}

func initStat() {
	statCmd.Flags().StringP("object-path", "p", "", "object path to show statistics for")
	statCmd.Flags().StringP("object-id", "i", "", "object id to show statistics for")

	infos := []string{}
	for info, _ := range object.StatInfoString {
		infos = append(infos, info)
	}
	statCmd.Flags().String("stat-info", "", fmt.Sprintf("comma separated list of info fields to show [%s]", strings.Join(infos, ",")))
}

// doStatConf updates the configuration based on the command line flags for the 'stat' command.
func doStatConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "object-path"); str != "" {
		conf.Stat.ObjectPath = str
	}
	if str := getFlagString(cmd, "object-id"); str != "" {
		conf.Stat.ObjectID = str
	}
	if str := getFlagString(cmd, "stat-info"); str != "" {
		conf.Stat.Info = []string{}
		for _, s := range strings.Split(str, ",") {
			conf.Stat.Info = append(conf.Stat.Info, strings.ToLower(strings.TrimSpace(s)))
		}
	}
}

// doStat is the main function for the 'stat' command.
// It retrieves and displays statistics for an OCFL structure or a specific object within it.
func doStat(cmd *cobra.Command, args []string) {
	ocflPath := args[0]

	// Update configuration based on flags
	doStatConf(cmd)

	oPath := conf.Stat.ObjectPath
	oID := conf.Stat.ObjectID
	if oPath != "" && oID != "" {
		emperror.Panic(cmd.Help())
		cobra.CheckErr(errors.New("do not use object-path AND object-id at the same time"))
		return
	}

	statInfo := []object.StatInfo{}
	for _, statInfoString := range conf.Stat.Info {
		statInfoString = strings.ToLower(strings.TrimSpace(statInfoString))
		var found bool
		for str, info := range object.StatInfoString {
			if strings.ToLower(str) == statInfoString {
				found = true
				statInfo = append(statInfo, info)
			}
		}
		if !found {
			emperror.Panic(cmd.Help())
			cobra.CheckErr(errors.Errorf("--stat-info invalid value '%s' ", statInfoString))
		}
	}

	logger.Info().Msgf("opening '%s'", ocflPath)

	ocflPath = writefs.RealPath(vfs, ocflPath)

	// Prepare access to the OCFL directory
	_destFS, err := writefs.Sub(vfs, ocflPath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
		return
	}
	destFS, ok := _destFS.(appendfs.FS)
	if !ok {
		logger.Error().Msgf("filesystem '%s' is not a writeable", ocflPath)
		return
	}
	defer func() {
		if err := writefs.Close(destFS); err != nil {
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", destFS)
		}
	}()

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return
	}

	// Setup extension manager for storage root
	_, _, err = initocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return
	}

	// Load the storage root
	storageRoot, err := initocfl.LoadStorageRoot(ctx, destFS, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return
	}

	if err := storageRoot.Stat(os.Stdout, oPath, oID, statInfo); err != nil {
		logger.Error().Err(err).Msg("cannot get statistics")
		return
	}
	_ = showStatus(logger)
}
