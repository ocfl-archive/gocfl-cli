package tests

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	defaultextensions_object "github.com/ocfl-archive/gocfl-cli/data/defaultextensions/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/rs/zerolog"
)

func TestFixtures11(t *testing.T) {
	fixtureRoot := "../../../fixtures/1.1"
	absFixtureRoot, err := filepath.Abs(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(absFixtureRoot); os.IsNotExist(err) {
		t.Skipf("fixtures not found at %s", absFixtureRoot)
	}

	subdirs := []string{"bad-objects", "warn-objects", "good-objects"}

	reCode := regexp.MustCompile(`[WE]\d{3}`)

	for _, subdir := range subdirs {
		dirPath := filepath.Join(absFixtureRoot, subdir)
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			t.Errorf("cannot read dir %s: %v", dirPath, err)
			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			t.Run(filepath.Join(subdir, entry.Name()), func(t *testing.T) {
				expectedCodes := reCode.FindAllString(entry.Name(), -1)
				path := filepath.Join(dirPath, entry.Name())

				// Setup components
				ctx := context.Background()
				zlog := zerolog.New(io.Discard)
				logger := ocfl.NewOCFLLogger(ctx, &zlog, nil, version.Version1_1, nil)

				extManager, _, err := ocfl.SetupExtensionManager[object.ExtensionManager](nil, defaultextensions_object.DefaultObjectExtensionFS, logger)
				if err != nil {
					t.Fatalf("cannot setup extension manager: %v", err)
				}
				defer extManager.Terminate()

				fsys := os.DirFS(path)
				obj, err := ocfl.LoadObject(ctx, fsys, nil, logger)
				if err == nil {
					defer obj.Close()
					validator := obj.GetValidator()
					defer validator.Close()
					_ = validator.Validate()
				}

				validationErrors := logger.ValidationErrors()
				foundCodes := make(map[string]bool)
				for _, vErr := range validationErrors {
					foundCodes[string(vErr.Code)] = true
				}

				// If we have no expected codes, it's a good object or at least we don't expect specific errors
				for _, expected := range expectedCodes {
					if !foundCodes[expected] {
						var found []string
						for c := range foundCodes {
							found = append(found, c)
						}
						t.Errorf("expected code %s not found. Found codes: %v", expected, found)
					}
				}

				if subdir == "good-objects" {
					hasError := false
					var found []string
					for _, vErr := range validationErrors {
						// Filter out W000 and E001 (which is noise due to missing extensions folder in these fixtures)
						if vErr.Code != "W000" && vErr.Code != "E001" && (vErr.Code[0] == 'E' || vErr.Code[0] == 'W') {
							hasError = true
							found = append(found, string(vErr.Code))
						}
					}
					if hasError {
						t.Errorf("good object should have no errors/warnings (except W000/E001), but found: %v", found)
					}
				}
			})
		}
	}
}
