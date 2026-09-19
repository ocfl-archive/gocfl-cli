package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"emperror.dev/errors"
	iop "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl-cli/data/displaydata"
	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd/display"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
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
	RunE:    doDisplay,
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
	displayCmd.Flags().StringP("display-fullreport", "r", "", "path to pdf file with full report")
	displayCmd.Flags().StringP("display-id", "i", "", "id of the report to display")
	displayCmd.MarkFlagsRequiredTogether("display-fullreport", "display-id")
}

// doDisplayConf updates the configuration based on the command line flags for the 'display' command.
func doDisplayConf(cmd *cobra.Command) error {
	if str := getFlagString(cmd, "display-addr"); str != "" {
		conf.Display.Addr = str
	}
	if str := getFlagString(cmd, "display-external-addr"); str != "" {
		conf.Display.AddrExt = str
	}
	if str := getFlagString(cmd, "display-templates"); str != "" {
		if err := conf.Display.Templates.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid display-templates '%s' for flag 'display-templates' or 'Display.Templates' config file entry", str)
			return errors.Wrapf(err, "invalid display-templates '%s'", str)
		}
	}
	if str := getFlagString(cmd, "display-tls-cert"); str != "" {
		if err := conf.Display.CertFile.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid display-tls-cert '%s' for flag 'display-tls-cert' or 'Display.CertFile' config file entry", str)
			return errors.Wrapf(err, "invalid display-tls-cert '%s'", str)
		}
	}
	if str := getFlagString(cmd, "display-tls-key"); str != "" {
		if err := conf.Display.KeyFile.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid display-tls-key '%s' for flag 'display-tls-key' or 'Display.KeyFile' config file entry", str)
			return errors.Wrapf(err, "invalid display-tls-key '%s'", str)
		}
	}
	if str := getFlagString(cmd, "display-fullreport"); str != "" {
		if err := conf.Display.Report.UnmarshalText([]byte(str)); err != nil {
			logger.Error().Err(err).Msgf("invalid display-fullreport '%s' for flag 'display-fullreport' or 'Display.Report' config file entry", str)
			return errors.Wrapf(err, "invalid display-fullreport '%s'", str)
		}
	}
	if str := getFlagString(cmd, "display-id"); str != "" {
		conf.Display.Id = str
	}
	return nil
}

// doDisplay is the main function for the 'display' command.
// It starts a web server to display the content of an OCFL structure.
func doDisplay(cmd *cobra.Command, args []string) error {
	ocflPath := args[0]

	// Update configuration based on flags
	if err := doDisplayConf(cmd); err != nil {
		return err
	}

	logger.Info().Msgf("opening '%s'", ocflPath)

	ocflPath = writefs.RealPath(vfs, ocflPath)

	// Prepare access to the OCFL directory or zip file
	var destFS fs.FS
	if strings.ToLower(path.Ext(ocflPath)) == ".zip" {
		zipFS, err := zipfs.NewFSFile(vfs, ocflPath, logger.Logger())
		if err != nil {
			logger.Error().Err(err).Msgf("cannot open zip filesystem at '%s'", ocflPath)
			return errors.Wrapf(err, "cannot open zip filesystem at '%s'", ocflPath)
		}
		defer zipFS.Close()
		destFS = zipFS
	} else {
		var err error
		destFS, err = writefs.Sub(vfs, ocflPath)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot get filesystem for '%s'", ocflPath)
			return errors.Wrapf(err, "cannot get filesystem for '%s'", ocflPath)
		}
		defer func() {
			if err := writefs.Close(destFS); err != nil {
				logger.Error().Err(err).Msgf("cannot close filesystem for '%s'", destFS)
			}
		}()
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

	// Load storage root in read-only mode
	storageRoot, err := ocfl.LoadStorageRoot(ctx, destFS, nil, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot load storage root")
		return errors.Wrap(err, "cannot load storage root")
	}
	defer storageRoot.Close()

	objectExtensionManager, objectExtensionFactory, err := ocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, firstOrSecond(conf.Add.ObjectExtensionFolder == "", (fs.FS)(defaultextensions_object.DefaultObjectExtensionFS), os.DirFS(conf.Add.ObjectExtensionFolder.String())), logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return errors.Wrap(err, "cannot setup object extension manager")
	}
	defer func() {
		if err := objectExtensionManager.Terminate(); err != nil {
			logger.Error().Err(err).Msg("cannot terminate object extension manager")
		}
	}()

	// Setup display server
	urlC, _ := url.Parse(conf.Display.AddrExt)
	var templateFS fs.FS
	if conf.Display.Templates == "" {
		templateFS, err = writefs.Sub(displaydata.TemplateRoot, "templates")
		if err != nil {
			logger.Error().Err(err).Msg("cannot get templates")
			return errors.Wrap(err, "cannot get templates")
		}
	} else {
		templateFS = os.DirFS(conf.Display.Templates.String())
	}
	srv, err := display.NewServer(storageRoot, objectExtensionFactory, "gocfl", conf.Display.Addr, urlC, displaydata.WebRoot, templateFS, conf.Display.Report.String(), conf.Display.Id, logger, io.Discard)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create server")
		return errors.Wrap(err, "cannot create server")
	}

	go func() {
		if err := srv.ListenAndServe("", ""); err != nil {
			logger.Error().Err(err).Msgf("cannot start server")
			return
		}
	}()
	defer srv.Shutdown(context.Background())
	if conf.Display.Report == "" {
		done := make(chan os.Signal, 1)
		signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		fmt.Println("press ctrl+c to stop server")
		s0 := <-done
		fmt.Println("got signal:", s0)
		return nil
	}
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("no-sandbox", true), // Wichtig für manche Linux-User/Root-Modi
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancelAlloc()

	// Kontext mit Allocator erstellen
	ctx, cancelCtx := chromedp.NewContext(allocCtx)
	defer cancelCtx()

	u, err := url.JoinPath(srv.HTTPAddr, "/object/id", conf.Display.Id, "/report")
	if err != nil {
		logger.Error().Err(err).Msg("Fehler beim Erstellen der URL")
		return errors.Wrap(err, "Fehler beim Erstellen der URL")
	}
	u += "?full&polyfilled=false"

	if err := chromedp.Run(ctx,
		chromedp.Navigate(u),
		chromedp.ActionFunc(func(ctx context.Context) error {
			p := page.PrintToPDF().
				WithPrintBackground(true).
				WithPaperWidth(8.27).
				WithPaperHeight(11.69).
				WithTransferMode(page.PrintToPDFTransferModeReturnAsStream)

			_, streamID, err := p.Do(ctx)
			if err != nil {
				logger.Error().Err(err).Msgf("Fehler beim Erstellen des PDFs - %s", u)
				return err
			}
			defer iop.Close(streamID).Do(context.Background())

			// 2. Ausgabedatei erstellen
			file, err := os.Create(conf.Display.Report.String())
			if err != nil {
				logger.Error().Err(err).Msgf("Fehler beim Erstellen der Ausgabedatei %s", conf.Display.Report.String())
				return errors.Wrapf(err, "Fehler beim Erstellen der Ausgabedatei %s", conf.Display.Report.String())
			}
			defer file.Close()
			for {
				data, eof, err := iop.Read(streamID).Do(ctx)
				if err != nil {
					logger.Error().Err(err).Msg("Fehler beim Lesen des PDFs")
					return errors.Wrap(err, "Fehler beim Lesen des PDFs")
				}

				if len(data) > 0 {
					// WICHTIG: DevTools liefert Stream-Daten base64-kodiert aus!
					decoded := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
					n, err := base64.StdEncoding.Decode(decoded, []byte(data))
					if err != nil {
						logger.Error().Err(err).Msg("Fehler beim Decodieren der base64-Daten")
						return errors.Wrap(err, "Fehler beim Decodieren der base64-Daten")
					}
					if _, err := file.Write(decoded[:n]); err != nil {
						logger.Error().Err(err).Msg("Fehler beim Schreiben des PDFs")
						return errors.Wrap(err, "Fehler beim Schreiben des PDFs")
					}
				}

				if eof {
					break
				}
			}
			return file.Sync()
		}),
	); err != nil {
		logger.Error().Err(err).Msgf("Fehler beim Erstellen des PDFs - %s", u)
		return errors.Wrapf(err, "Fehler beim Erstellen des PDFs - %s", u)
	}

	logger.Info().Msgf("PDF %s erfolgreich erstellt!", conf.Display.Report.String())
	logger.Info().Msg("server stopped")
	return nil
}
