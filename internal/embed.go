package internal

import (
	"embed"
)

//go:embed errors.toml default.toml
//go:embed extensions/object/*/* extensions/storageroot/*/*
//go:embed thumbnail/scripts/* thumbnail/thumbnail.toml
var InternalFS embed.FS
