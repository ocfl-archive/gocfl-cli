package cmd

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl/v3/pkg/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/validation"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:     "test [path to folder with test fixtures]",
	Aliases: []string{"fixtures"},
	Short:   "check ocfl fixtures",
	Long:    "check gocfl against folder with test fixtures. Every folder contains one fixture object. If folder name starts with validation codes it's checked, whether they are found.",
	Example: "gocfl test <path to ocfl test fixtures>",
	Args:    cobra.MaximumNArgs(1),
	Run:     doTest,
}

func initTest() {
	testCmd.Flags().StringP("object-path", "p", "", "folder of fixture")
}

// doTestConf updates the configuration based on the command line flags for the 'test' command.
func doTestConf(cmd *cobra.Command) {
	if str := getFlagString(cmd, "object-path"); str != "" {
		conf.Test.ObjectPath = str
	}
}

// doTest is the main function for the 'test' command.
// It runs validation tests against OCFL test fixtures in a specified folder.
func doTest(cmd *cobra.Command, args []string) {
	if len(args) > 0 && len(args[0]) > 0 {
		conf.Test.FixturePath = args[0]
	}

	// Update configuration based on flags
	doTestConf(cmd)

	fixturePath := conf.Test.FixturePath
	fixturePath = writefs.RealPath(vfs, fixturePath)
	logger.Info().Msgf("vfs created : %v", vfs)

	logger.Info().Msgf("opening '%s'", fixturePath)

	extensionParams, err := getExtensionParams(cmd)
	if err != nil {
		logger.Error().Err(err).Msg("cannot get extension params")
		return
	}

	// Setup object extension manager
	_, _, err = initocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, nil, logger)
	if err != nil {
		logger.Error().Err(err).Msg("cannot setup object extension manager")
		return
	}

	dirs, err := fs.ReadDir(vfs, fixturePath)
	if err != nil {
		logger.Error().Err(err).Msgf("cannot read dir '%s'", fixturePath)
		return
	}
	for _, dir := range dirs {
		folderName := dir.Name()
		if conf.Test.ObjectPath != "" && folderName != conf.Test.ObjectPath {
			logger.Debug().Msgf("ignoring dir '%s'", folderName)
			continue
		}
		logger.Info().Msgf("dir: %s", folderName)
		if err := func() error {

			objFsys, err := writefs.Sub(vfs, path.Join(fixturePath, folderName))
			if err != nil {
				return errors.Wrapf(err, "cannot open ocfl filesystem '%s'", fixturePath)
			}

			obj, err := initocfl.LoadObject(ctx, objFsys, nil, logger)
			if err != nil {
				return errors.Wrapf(err, "cannot load object '%v'", objFsys)
			}
			defer obj.Close()

			checker := obj.GetChecker()
			if err := checker.Check(); err != nil {
				return errors.Wrapf(err, "cannot check object '%v'", objFsys)
			}
			return nil
		}(); err != nil {
			logger.Error().Err(err).Msgf("cannot validate object '%v'", folderName)
		}
		contextString := ""
		errs := 0
		for _, err := range logger.ValidationErrors() {
			if err.Code[0] == 'E' {
				errs++
			}
			if err.Context != contextString {
				fmt.Printf("[%s] [%s]\n", folderName, err.Context)
				contextString = err.Context
			}
			fmt.Printf("   #%s - %s\n", err.Code, err.Description)
		}
		if errs > 0 {
			fmt.Printf("\n%d errors found\n", errs)
		} else {
			fmt.Printf("\nno errors found\n")
		}
		errorList := errorsFromFolder(folderName)
		errorNotFound := []string{}
		validationErrors := logger.ValidationErrors()
		for _, errNo := range errorList {
			var found = false
			var allErrors = map[validation.ErrorCode]string{}
			for _, err := range validationErrors {
				allErrors[err.Code] = err.Description
			}
			for code, desc := range allErrors {
				if code == validation.ErrorCode(errNo) {
					fmt.Printf("Error found:   #%s - %s\n", code, desc)
					found = true
					continue
				}
			}
			if !found {
				errorNotFound = append(errorNotFound, errNo)
			}
		}
		if len(errorNotFound) > 0 {
			fmt.Printf("[%s] Errors not found: %v\n", folderName, errorNotFound)
			//return
		} else if len(errorList) == 0 && len(validationErrors) > 0 {
			fmt.Printf("[%s] Errors found, but object should be valid\n", folderName)
			//return
		} else {
			fmt.Printf("[%s] All errors found\n", folderName)
		}
		logger.ClearValidationErrors()
	}
}

var folderErrorRegexp = regexp.MustCompile(`^((?:[EW]\d{3}_)+)`)
var errorCodeRegexp = regexp.MustCompile(`([EW]\d{3})`)

func errorsFromFolder(folder string) []string {
	matches := folderErrorRegexp.FindStringSubmatch(folder)
	if len(matches) < 2 {
		return nil
	}
	errorMatches := errorCodeRegexp.FindAllString(matches[1], -1)
	return errorMatches
}
