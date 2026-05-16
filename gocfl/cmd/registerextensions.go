package cmd

// register all extensions here
import (
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0001_digest_algorithms"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0002_flat_direct_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0003_hash_and_id_n_tuple_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0004_hashed_n_tuple_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0006_flat_omit_prefix_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0007_n_tuple_omit_prefix_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0009_digest_algorithms"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0011_direct_clean_path_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_content_subpath"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_filesystem"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_mets"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_pairtree_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_timestamp"
)
