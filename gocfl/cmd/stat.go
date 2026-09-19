package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
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
	RunE:    doStat,
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
func doStatConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "object-path"); str != "" {
		if err := conf.Stat.ObjectPath.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid object-path '%s' for flag 'object-path' or 'Stat.ObjectPath' config file entry", str)
			return errors.Wrapf(err, "invalid object-path '%s'", str)
		}
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
	return nil
}

// doStat is the main function for the 'stat' command.
// It retrieves and displays statistics for an OCFL structure or a specific object within it.
func doStat(cmd *cobra.Command, args []string) error {
	ocflPath := args[0]

	// Update configuration based on flags
	if err := doStatConf(cmd); err != nil {
		return err
	}

	oPath := conf.Stat.ObjectPath.String()
	oID := conf.Stat.ObjectID
	if oPath != "" && oID != "" {
		_ = cmd.Help()
		return errors.New("do not use object-path AND object-id at the same time")
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
			_ = cmd.Help()
			return errors.Errorf("--stat-info invalid value '%s'", statInfoString)
		}
	}

	logger.Info().Msgf("opening '%s'", ocflPath)

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
			logger.Error().Err(err).Msgf("cannot close filesystem '%s'", ocflPath)
		}
	}()

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return errors.Wrap(err, "cannot get extension params")
	}

	// Setup extension manager for storage root
	_, _, err = ocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return errors.Wrap(err, "cannot setup storage root extension manager")
	}

	// Load the storage root
	storageRoot, err := ocfl.LoadStorageRoot(ctx, destFS, nil, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return errors.Wrap(err, "cannot load storage root")
	}
	defer storageRoot.Close()

	if err := storageRoot.Stat(os.Stdout, oPath, oID, statInfo); err != nil {
		logger.Error().Err(err).Msg("cannot get statistics")
		return errors.Wrap(err, "cannot get statistics")
	}
	if showStatus(logger) {
		return errors.New("stat failed")
	}
	return nil
}
