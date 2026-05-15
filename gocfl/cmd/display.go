package cmd

import (
	"context"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/je4/filesystem/v4/pkg/writefs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl-cli/data/displaydata"
	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd/display"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/spf13/cobra"
)

var displayCmd = &cobra.Command{
	Use:     "display [path to ocfl structure]",
	Aliases: []string{"viewer"},
	Short:   "show content of ocfl object in webbrowser",
	//Long:    "an utterly useless command for testing",
	Example: "gocfl display ./archive.zip",
	Args:    cobra.MinimumNArgs(1),
	Run:     doDisplay,
}

/*
[Display]
# --display-addr
Addr = "localhost:8080"
# --display-external-addr
ExternalAddr = "http://localhost:8080"
# --display-templates
Templates = "./data/displaydata/templates"
*/

func initDisplay() {
	displayCmd.Flags().StringP("display-addr", "a", "localhost:8080", "address to listen on")
	displayCmd.Flags().StringP("display-external-addr", "e", "http://localhost:8080", "external address to access the server")
	displayCmd.Flags().StringP("display-templates", "t", "", "path to templates")
	displayCmd.Flags().StringP("display-tls-cert", "c", "", "path to tls certificate")
	displayCmd.Flags().StringP("display-tls-key", "k", "", "path to tls certificate key")
}

// doDisplayConf updates the configuration based on the command line flags for the 'display' command.
func doDisplayConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "display-addr"); str != "" {
		conf.Display.Addr = str
	}
	if str := getFlagString(cmd, "display-external-addr"); str != "" {
		conf.Display.AddrExt = str
	}
	if str := getFlagString(cmd, "display-templates"); str != "" {
		conf.Display.Templates = str
	}
	if str := getFlagString(cmd, "display-tls-cert"); str != "" {
		conf.Display.CertFile = str
	}
	if str := getFlagString(cmd, "display-tls-key"); str != "" {
		conf.Display.KeyFile = str
	}
}

// doDisplay is the main function for the 'display' command.
// It starts a web server to display the content of an OCFL structure.
func doDisplay(cmd *cobra.Command, args []string) {
	ocflPath := args[0]

	// Update configuration based on flags
	doDisplayConf(cmd)

	logger.Info().Msgf("opening '%s'", ocflPath)

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

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return
	}

	// Setup extension managers for storage root and object
	_, _, err = initocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup storage root extension manager")
		return
	}

	// Load storage root in read-only mode
	storageRoot, err := initocfl.LoadStorageRoot(ctx, destFS, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return
	}

	objectExtensionManager, objectExtensionFactory, err := initocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder)), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate storage root extension manager")
		}
	}()

	// Setup display server
	urlC, _ := url.Parse(conf.Display.AddrExt)
	var templateFS fs.FS
	if conf.Display.Templates == "" {
		templateFS, err = writefs.Sub(displaydata.TemplateRoot, "templates")
		if err != nil {
			logger.Error().Err(err).Msg("cannot get templates")
			return
		}
	} else {
		templateFS = os.DirFS(conf.Display.Templates)
	}
	srv, err := display.NewServer(storageRoot, objectExtensionFactory, "gocfl", conf.Display.Addr, urlC, displaydata.WebRoot, templateFS, logger, io.Discard)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create server")
		return
	}

	go func() {
		if err := srv.ListenAndServe("", ""); err != nil {
			logger.Error().Err(err).Msgf("cannot start server")
			return
		}
	}()

	end := make(chan bool, 1)

	// process waiting for interrupt signal (TERM or KILL)
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)

		signal.Notify(sigint, syscall.SIGTERM)
		signal.Notify(sigint, syscall.SIGKILL)

		<-sigint

		// We received an interrupt signal, shut down.
		logger.Info().Msg("interrupt signal received")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		srv.Shutdown(ctx)

		end <- true
	}()

	<-end
	logger.Info().Msg("server stopped")

}
