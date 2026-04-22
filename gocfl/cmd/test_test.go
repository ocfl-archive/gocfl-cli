package cmd

import (
	"reflect"
	"testing"
)

func TestErrorsFromFolder(t *testing.T) {
	tests := []struct {
		folder   string
		expected []string
	}{
		{"E001_extra_dir_in_root", []string{"E001"}},
		{"E008_W036_no_versions_no_head", []string{"E008", "W036"}},
		{"W123_E456_something", []string{"W123", "E456"}},
		{"no_error_here", nil},
		{"E12_not_enough_digits", nil},
		{"X001_wrong_letter", nil},
		{"E001_W002_W003_many", []string{"E001", "W002", "W003"}},
	}

	for _, tt := range tests {
		t.Run(tt.folder, func(t *testing.T) {
			actual := errorsFromFolder(tt.folder)
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("errorsFromFolder(%q) = %v, want %v", tt.folder, actual, tt.expected)
			}
		})
	}
}
