//go:build !imagick && !vips

package main

import (
	"github.com/ocfl-archive/gocfl-cli/gocfl/cmd"
)

/*
func init() {
	os.Setenv("SIEGFRIED_HOME", "c:/temp")
}
*/

func main() {
	cmd.Execute()
}
