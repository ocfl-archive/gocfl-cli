package cmd

import (
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-cli/internal"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/util"
	"github.com/spf13/cobra"
)

func quoteCmdArg(s string) string {
	// Einfacher Ansatz für cmd.exe: Anführungszeichen um das Argument,
	// innere Quotes verdoppeln.
	s = strings.ReplaceAll(s, `"`, `""`)
	return `"` + s + `"`
}

func quoteShellArg(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

var initConfigCmd = &cobra.Command{
	Use:     "initconfig [path to config file]",
	Aliases: []string{},
	Short:   "store configuration of gocfl in toml format",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl initconfig",
	Args:    cobra.MaximumNArgs(1),
	Run:     doInitConfig,
}

func initInitConfig() {
	initConfigCmd.Flags().String("toml", "", "name of toml config file")
	initConfigCmd.Flags().String("extension-folder", "", "folder for extension templates")
	initConfigCmd.Flags().String("script-folder", "", "folder for extension scripts")
	initConfigCmd.Flags().Bool("fullconfig", false, "store all configuration options instead of minimal configuration")
	initConfigCmd.Flags().Bool("extensions", false, "extract extension templates")
	initConfigCmd.Flags().Bool("scripts", false, "extract extension scripts")
}

// doInitConfigConf updates the configuration based on the command line flags for the 'initconfig' command.
func doInitConfigConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "toml"); str != "" {
		conf.InitConfig.TOMLFile = configutil.Path(str)
	}
	if str := getFlagString(cmd, "extension-folder"); str != "" {
		conf.InitConfig.ExtensionFolder = configutil.Path(str)
	}
	if str := getFlagString(cmd, "script-folder"); str != "" {
		conf.InitConfig.ScriptFolder = configutil.Path(str)
	}
	if b, ok := getFlagBool(cmd, "fullconfig"); ok {
		conf.InitConfig.FullConfig = b
	}
	if b, ok := getFlagBool(cmd, "extensions"); ok {
		conf.InitConfig.Extensions = b
	}
	if b, ok := getFlagBool(cmd, "scripts"); ok {
		conf.InitConfig.Scripts = b
	}

}

// doInitConfig is the main function for the 'initconfig' command.
// It stores the current configuration of gocfl in TOML format and optionally extracts extension templates and scripts.
func doInitConfig(cmd *cobra.Command, args []string) {
	var configFolder string
	var err error
	if len(args) == 0 {
		configFolder = conf.InitConfig.ConfigFolder.String()
	} else {
		configFolder = args[0]
	}
	configFolder, err = util.Fullpath(configFolder)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot get full path to config folder '%s'", configFolder)
	}
	configFolder = filepath.ToSlash(configFolder)
	logger.Info().Msgf("Config Folder: %s", configFolder)

	// Update configuration based on flags
	doInitConfigConf(cmd)

	var scriptFolder = conf.InitConfig.ScriptFolder.String()
	var extensionFolder = conf.InitConfig.ExtensionFolder.String()
	var tomlPath = conf.InitConfig.TOMLFile.String()
	if !filepath.IsAbs(scriptFolder) {
		scriptFolder = filepath.ToSlash(filepath.Join(configFolder, scriptFolder))
	}
	logger.Info().Msgf("Script Folder: %s", scriptFolder)
	if !filepath.IsAbs(extensionFolder) {
		extensionFolder = filepath.ToSlash(filepath.Join(configFolder, extensionFolder))
	}
	if !filepath.IsAbs(scriptFolder) {
		scriptFolder = filepath.ToSlash(filepath.Join(configFolder, scriptFolder))
	}
	logger.Info().Msgf("Extension Folder: %s", extensionFolder)
	if !filepath.IsAbs(tomlPath) {
		tomlPath = filepath.ToSlash(filepath.Join(configFolder, tomlPath))
	}
	logger.Info().Msgf("TOML File: %s", tomlPath)

	//	scripts := []string{}
	newMiniConfig := configutil.MiniConfig{
		"log.level":      conf.Log.Level,
		"add.user":       conf.Add.User,
		"add.message":    conf.Add.Message,
		"update.user":    conf.Update.User,
		"update.message": conf.Update.Message,
	}

	if conf.InitConfig.Extensions {
		if err := os.MkdirAll(extensionFolder, 0755); err != nil {
			logger.Fatal().Err(err).Msgf("cannot create extension folder: %s", extensionFolder)
		}
		extFS, err := writefs.Sub(internal.InternalFS, "extensions")
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create subfs for internal:extensions")
		}
		if err := fs.WalkDir(extFS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return errors.WithStack(err)
			}
			if d.IsDir() {
				return nil
			}
			target := filepath.Join(extensionFolder, path)
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return errors.Wrapf(err, "cannot create directory: %s", filepath.Dir(target))
			}
			src, err := extFS.Open(path)
			if err != nil {
				return errors.Wrapf(err, "cannot open file: internal:%s", path)
			}
			defer func(src fs.File) {
				err := src.Close()
				if err != nil {
					logger.Error().Err(err).Msgf("cannot close file: internal:%s", path)
				}
			}(src)
			out, err := os.Create(target)
			if err != nil {
				return errors.Wrapf(err, "cannot create file: %s", target)
			}
			defer func(out *os.File) {
				err := out.Close()
				if err != nil {
					logger.Error().Err(err).Msgf("cannot close file: %s", target)
				}
			}(out)
			logger.Info().Msgf("copying extension config: internal:%s -> %s", path, target)
			if _, err := io.Copy(out, src); err != nil {
				return errors.Wrapf(err, "cannot copy file: internal:%s -> %s", path, target)
			}
			return nil
		}); err != nil {
			logger.Fatal().Err(err).Msg("cannot walk internal:extensions")
		}

		conf.Init.StorageRootExtensionFolder = configutil.Path(filepath.ToSlash(filepath.Join(extensionFolder, "storageroot")))
		newMiniConfig["init.storagerootextensions"] = conf.Init.StorageRootExtensionFolder

		conf.Add.ObjectExtensionFolder = configutil.Path(filepath.ToSlash(filepath.Join(extensionFolder, "object")))
		newMiniConfig["add.objectextensions"] = conf.Add.ObjectExtensionFolder
	}
	thumbConf, thumbMiniconfig, err := ext_NNNN_thumbnail.InitConfig(conf.Thumbnail, scriptFolder, logger.Logger())
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot init thumbnail")
	}
	for k, v := range thumbMiniconfig {
		newMiniConfig["thumbnail."+k] = v
	}

	conf.Thumbnail = thumbConf
	if err := os.MkdirAll(filepath.Dir(tomlPath), 0755); err != nil {
		logger.Fatal().Err(err).Msgf("cannot create thumbnail directory: %s", filepath.Dir(tomlPath))
	}
	fp, err := os.Create(tomlPath)
	if err != nil {
		log.Fatalf("cannot create config file: %v", err)
	}
	defer func(fp *os.File) {
		err := fp.Close()
		if err != nil {
			logger.Error().Msgf("cannot close config file: %v", err)
		}
	}(fp)
	for k, v := range newMiniConfig {
		miniConfig[k] = v
	}
	var buf []byte
	if conf.InitConfig.FullConfig {
		buf, err = toml.Marshal(conf)
		if err != nil {
			logger.Fatal().Msgf("cannot encode config: %v", err)
		}
	} else {
		buf, err = toml.Marshal(miniConfig)
		if err != nil {
			logger.Fatal().Msgf("cannot encode config: %v", err)
		}
	}
	if err := os.WriteFile(tomlPath, buf, 0644); err != nil {
		cobra.CheckErr(errors.Errorf("cannot write config file: %v", err))
	}
}
