package display

import (
	"context"
	"crypto/tls"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/Masterminds/sprig/v3"
	"github.com/dustin/go-humanize"
	"github.com/gin-contrib/multitemplate"
	"github.com/gin-gonic/gin"
	dcert "github.com/je4/utils/v2/pkg/cert"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_content_subpath"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_filesystem"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	extensiontypes "github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
	objecttypes "github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/ocfl-archive/indexer/v3/pkg/indexer"
)

type Server struct {
	service          string
	host, port       string
	name, password   string
	srv              *http.Server
	linkTokenExp     time.Duration
	jwtKey           string
	jwtAlg           []string
	log              ocfllogger.OCFLLogger
	urlExt           *url.URL
	accessLog        io.Writer
	dataFS           fs.FS
	storageRoot      storageroot.StorageRoot
	object           objecttypes.Object
	metadata         *inventory.Metadata
	templateFS       fs.FS
	obfuscate        bool
	httpObjectFS     http.FileSystem
	extensionFactory extensiontypes.Factory[objecttypes.ExtensionManager]
	objectFS         fs.FS
	reportfile       string
	id               string
	HTTPAddr         string
	reportAreas      []string
}

func NewServer(storageRoot storageroot.StorageRoot, extensionFactory extensiontypes.Factory[objecttypes.ExtensionManager], service, addr string, urlExt *url.URL, dataFS, templateFS fs.FS, report, id string, reportareas []string, log ocfllogger.OCFLLogger, accessLog io.Writer) (*Server, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot split address %s", addr)
	}

	scheme := "http"
	if urlExt != nil && urlExt.Scheme != "" {
		scheme = urlExt.Scheme
	}
	displayHost := host
	if displayHost == "" || displayHost == "0.0.0.0" {
		displayHost = "localhost"
	}
	httpAddr := fmt.Sprintf("%s://%s:%s", scheme, displayHost, port)

	srv := &Server{
		extensionFactory: extensionFactory,
		service:          service,
		host:             host,
		port:             port,
		urlExt:           urlExt,
		dataFS:           dataFS,
		templateFS:       templateFS,
		log:              log,
		accessLog:        accessLog,
		storageRoot:      storageRoot,
		reportfile:       report,
		id:               id,
		HTTPAddr:         httpAddr,
		reportAreas:      reportareas,
	}

	return srv, nil
}

func (s *Server) ListenAndServe(cert, key string) (err error) {
	gin.SetMode(gin.ReleaseMode)
	route := gin.Default()
	route.UseRawPath = true
	route.UnescapePathValues = false

	route.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	mt := multitemplate.New()

	var tplfiles []string = []string{
		"storageroot.gohtml",
		"object.gohtml",
		"manifest.gohtml",
		"version.gohtml",
		"detail.gohtml",
		"report.gohtml",
	}

	for _, tplfile := range tplfiles {
		funcMap := sprig.FuncMap()
		funcMap["basename"] = func(str string) string {
			return filepath.Base(str)
		}
		funcMap["PathEscape"] = func(str string) string {
			return url.PathEscape(str)
		}
		funcMap["humanizeBytes"] = func(size uint64) string {
			return humanize.Bytes(size)
		}
		funcMap["humanizeTime"] = func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		}

		tpl, err := template.New(tplfile).Funcs(funcMap).ParseFS(s.templateFS, tplfile)
		if err != nil {
			return errors.Wrapf(err, "cannot parse template %s", tplfile)
		}
		mt.Add(tplfile, tpl)
	}

	route.HTMLRender = mt
	route.GET("/", s.storageroot)
	//	route.GET("/:id", s.dashboard)
	route.GET("/object/id/:id", s.loadObjectID)
	route.GET("/object/id/:id/manifest", s.manifest)
	route.GET("/object/id/:id/version/:version", s.version)
	route.GET("/object/id/:id/detail/:checksum", s.detail)
	route.GET("/object/id/:id/report", s.report)
	route.GET("/object/id/:id/download/:checksum/:filename", s.download)
	route.GET("/object/id/:id/extension/:extension/download/*path", s.downloadExtFile)
	route.GET("/object/folder/*path", s.loadObjectPath)
	route.GET("/object/id/:id/browse/*path", s.loadObjectBrowser)

	route.StaticFS("/static", http.FS(s.dataFS))

	s.srv = &http.Server{
		Addr:    net.JoinHostPort(s.host, s.port),
		Handler: route.Handler(),
	}

	var tlsCert *tls.Certificate
	if cert == "auto" || key == "auto" {
		s.log.Info().Msg("generating new certificate")
		tlsCert, err = dcert.DefaultCertificate()
		if err != nil {
			return errors.Wrap(err, "cannot generate default certificate")
		}
	}
	proto := "http"
	if tlsCert != nil || (cert != "" && key != "") {
		proto = "https"
	}
	displayHost := s.host
	if displayHost == "" || displayHost == "0.0.0.0" {
		displayHost = "localhost"
	}
	s.HTTPAddr = fmt.Sprintf("%s://%s:%s", proto, displayHost, s.port)
	fmt.Printf("starting gocfl viewer at %v - %s/\n", s.urlExt.String(), s.HTTPAddr)
	if tlsCert != nil {
		s.srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*tlsCert}}
		if err := s.srv.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Msgf("server on %s exited with error", s.srv.Addr)
		}
	} else if cert != "" && key != "" {
		if err := s.srv.ListenAndServeTLS(cert, key); !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Msgf("server on %s exited with error", s.srv.Addr)
		}
	} else {
		if err := s.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			s.log.Error().Err(err).Msgf("server on %s exited with error", s.srv.Addr)
		}
	}
	return nil
}

func (s *Server) downloadExtFile(c *gin.Context) {
	var err error
	type idParam struct {
		ID        string `uri:"id" binding:"required"`
		Path      string `uri:"path" binding:"required"`
		Extension string `uri:"extension" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}
	iop.Path, err = url.PathUnescape(iop.Path)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.Path).Error()})
		return
	}
	iop.Extension, err = url.PathUnescape(iop.Extension)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.Extension).Error()})
		return
	}

	if s.object != nil && s.object.GetID() == iop.ID {
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID)})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetWriteFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()

		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}
	extractor := s.object.GetExtractor()
	defer extractor.Close()
	fp, size, contentType, err := extractor.GetExtensionFileReader(iop.Extension, iop.Path)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get file for object %s extension %s path %s", iop.ID, iop.Extension, iop.Path)})
	}
	defer fp.Close()
	c.DataFromReader(http.StatusOK, size, contentType, fp, map[string]string{})
}

func (s *Server) download(c *gin.Context) {
	var err error
	type idParam struct {
		ID       string `uri:"id" binding:"required"`
		Checksum string `uri:"checksum" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}

	if s.object != nil && s.object.GetID() == iop.ID {
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID).Error()})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}

	file, ok := s.metadata.Files[iop.Checksum]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("no file with checksum %s found", iop.Checksum).Error()})
		return
	}

	extractor := s.object.GetExtractor()
	defer extractor.Close()
	fp, size, mimetype, err := extractor.GetFileReader(file.InternalName[0])
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get file %s for object %s", file.InternalName[0], iop.ID).Error()})
		return
	}
	defer fp.Close()
	c.DataFromReader(http.StatusOK, size, mimetype, fp, map[string]string{})
}

func (s *Server) detail(c *gin.Context) {
	var err error
	type idParam struct {
		ID       string `uri:"id" binding:"required"`
		Checksum string `uri:"checksum" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}

	if s.object != nil && s.object.GetID() == iop.ID {
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID).Error()})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}

	file, ok := s.metadata.Files[iop.Checksum]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("no file with checksum %s found", iop.Checksum).Error()})
		return
	}
	type extFEntry struct {
		Name  string
		ATime string
		CTime string
		MTime string
		Size  string
		Attr  string
		OS    string
		Sys   string
	}
	type detailStatus struct {
		Checksum        string                              `json:"checksum"`
		DigestAlgorithm checksum.DigestAlgorithm            `json:"digestAlgorithm"`
		InternalNames   []string                            `json:"internalNames"`
		ExternalNames   map[string][]*extFEntry             `json:"externalNames"`
		Fixity          map[checksum.DigestAlgorithm]string `json:"fixity"`
		Indexer         *indexer.ResultV2                   `json:"indexer"`
		IndexerJSON     string
		Migration       *ext_NNNN_migration.MigrationResult
		Thumbnail       *ext_NNNN_thumbnail.ThumbnailResult
	}

	status := &detailStatus{
		Checksum:        iop.Checksum,
		DigestAlgorithm: s.metadata.DigestAlgorithm,
		InternalNames:   file.InternalName,
		ExternalNames:   map[string][]*extFEntry{},
		Fixity:          file.Checksums,
	}

	extFilesystemAny, _ := file.Extension[ext_NNNN_filesystem.FilesystemName]
	var extFilesystem map[string][]*ext_NNNN_filesystem.FileSystemLine
	if extFilesystemAny != nil {
		extFilesystem, _ = extFilesystemAny.(map[string][]*ext_NNNN_filesystem.FileSystemLine)
	}

	for ver, names := range file.VersionName {
		extFilesystemVersion, _ := extFilesystem[ver]
		if status.ExternalNames[ver] == nil {
			status.ExternalNames[ver] = []*extFEntry{}
		}
		for _, name := range names {
			efe := &extFEntry{
				Name: name,
			}
			if extFilesystemVersion != nil {
				for _, fs := range extFilesystemVersion {
					if fs.Path == name {
						efe.ATime = fs.Meta.ATime.Format(time.DateTime)
						efe.CTime = fs.Meta.CTime.Format(time.DateTime)
						efe.MTime = fs.Meta.MTime.Format(time.DateTime)
						efe.Size = humanize.Bytes(uint64(fs.Meta.Size))
						efe.Attr = fs.Meta.Attr
						efe.OS = fs.Meta.OS
						sys, _ := json.Marshal(fs.Meta.SystemStat, jsontext.WithIndent("  "))
						efe.Sys = string(sys)
						break
					}
				}
			}
			status.ExternalNames[ver] = append(status.ExternalNames[ver], efe)
		}
	}

	extIndexerAny, _ := file.Extension[ext_NNNN_indexer.IndexerName]
	var extIndexer *indexer.ResultV2
	if extIndexerAny != nil {
		extIndexer, _ = extIndexerAny.(*indexer.ResultV2)
	}

	if extIndexer != nil {
		iData, err := json.Marshal(extIndexer.Metadata, jsontext.WithIndent("  "))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot marshal indexer metadata for object %s", s.object.GetID()).Error()})
			return
		}
		//	extIndexer.Metadata = nil
		status.Indexer = extIndexer
		status.IndexerJSON = string(iData)
	}

	extMigrationAny, _ := file.Extension[ext_NNNN_migration.MigrationName]
	var extMigration *ext_NNNN_migration.MigrationResult
	if extMigrationAny != nil {
		extMigration, _ = extMigrationAny.(*ext_NNNN_migration.MigrationResult)
	}
	if extMigration != nil {
		status.Migration = extMigration
	}

	extThumbnailAny, _ := file.Extension[ext_NNNN_thumbnail.ThumbnailName]
	if extThumbnailAny != nil {
		if extThumbnail, ok := extThumbnailAny.(ext_NNNN_thumbnail.ThumbnailResult); ok {
			status.Thumbnail = &extThumbnail
		} else {
			if extThumbnail, ok := extThumbnailAny.(*ext_NNNN_thumbnail.ThumbnailResult); ok {
				status.Thumbnail = extThumbnail
			}
		}
	}

	var params = map[string]any{
		"title":  "Detail",
		"id":     s.object.GetID(),
		"status": status,
		//		"metadata": s.metadata,
		"file": file,
	}

	c.HTML(http.StatusOK, "detail.gohtml", gin.H(params))

}

func (s *Server) dashboard(c *gin.Context) {

	var id string
	if s.object != nil {
		id = s.object.GetID()
	}
	c.HTML(http.StatusOK, "object.gohtml", gin.H{
		"title": "gocfl",
		"id":    id,
	})
}

func (s *Server) storageroot(c *gin.Context) {

	var id string
	if s.object != nil {
		id = s.object.GetID()
	}

	if s.storageRoot == nil {
		c.JSON(http.StatusInternalServerError, "no storage root loaded")
		return
	}

	folders, err := s.storageRoot.GetObjectFolders()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.HTML(http.StatusOK, "storageroot.gohtml", gin.H{
		"title":       "gocfl",
		"id":          id,
		"folders":     folders,
		"storageroot": s.storageRoot.String(),
	})
}

func (s *Server) manifest(c *gin.Context) {
	var err error
	type idParam struct {
		ID string `uri:"id" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}

	if s.object != nil && s.object.GetID() == iop.ID {
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID).Error()})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}

	type fEntry struct {
		Checksum  string
		Pronom    string
		Mimetype  string
		IdxSize   string
		Migration map[string]string
	}
	var files = map[string]*fEntry{}
	var filenames = []string{}

	for checksum, file := range s.metadata.Files {
		extMigrationAny, _ := file.Extension[ext_NNNN_migration.MigrationName]
		var extMigration *ext_NNNN_migration.MigrationResult
		if extMigrationAny != nil {
			extMigration = extMigrationAny.(*ext_NNNN_migration.MigrationResult)
		}
		extIndexerAny, _ := file.Extension[ext_NNNN_indexer.IndexerName]
		var extIndexer *indexer.ResultV2
		if extIndexerAny != nil {
			extIndexer, _ = extIndexerAny.(*indexer.ResultV2)
		}

		for _, name := range file.InternalName {
			fe := &fEntry{
				Checksum:  checksum,
				Migration: map[string]string{},
			}
			if extIndexer != nil {
				fe.Pronom = extIndexer.Pronom
				fe.Mimetype = extIndexer.Mimetype
				fe.IdxSize = humanize.Bytes(extIndexer.Size)
			}
			if extMigration != nil {
				fe.Migration[extMigration.ID] = extMigration.Source
			}
			files[name] = fe
			filenames = append(filenames, name)
		}
	}

	var params = map[string]any{
		"title":     "Manifest",
		"id":        s.object.GetID(),
		"versions":  s.metadata.Versions,
		"files":     files,
		"filenames": filenames,
	}

	c.HTML(http.StatusOK, "manifest.gohtml", gin.H(params))

}

func (s *Server) version(c *gin.Context) {
	var err error
	type idParam struct {
		ID      string `uri:"id" binding:"required"`
		Version string `uri:"version" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}

	if s.object != nil && s.object.GetID() == iop.ID {
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID).Error()})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}

	type fEntry struct {
		CTime     string
		Size      string
		Checksum  string
		Pronom    string
		Mimetype  string
		IdxSize   string
		Attr      string
		OS        string
		Migration map[string]string
	}
	var files = map[string]*fEntry{}
	var filenames = []string{}

	for checksum, file := range s.metadata.Files {
		extMigrationAny, _ := file.Extension[ext_NNNN_migration.MigrationName]
		var extMigration *ext_NNNN_migration.MigrationResult
		if extMigrationAny != nil {
			extMigration = extMigrationAny.(*ext_NNNN_migration.MigrationResult)
		}
		extIndexerAny, _ := file.Extension[ext_NNNN_indexer.IndexerName]
		var extIndexer *indexer.ResultV2
		if extIndexerAny != nil {
			extIndexer, _ = extIndexerAny.(*indexer.ResultV2)
		}
		extFilesystemAny, _ := file.Extension[ext_NNNN_filesystem.FilesystemName]
		var extFilesystem map[string][]*ext_NNNN_filesystem.FileSystemLine
		if extFilesystemAny != nil {
			extFilesystem, _ = extFilesystemAny.(map[string][]*ext_NNNN_filesystem.FileSystemLine)
		}
		extFilesystemVersion, _ := extFilesystem[iop.Version]
		if vNames, ok := file.VersionName[iop.Version]; ok {
			for _, name := range vNames {
				fe := &fEntry{
					Checksum:  checksum,
					Migration: map[string]string{},
				}
				if extMigration != nil {
					fe.Migration[extMigration.ID] = extMigration.Source
				}
				if extFilesystemVersion != nil {
					for _, fsLine := range extFilesystemVersion {
						if fsLine.Path == name {
							fe.Size = humanize.Bytes(fsLine.Meta.Size)
							fe.CTime = fsLine.Meta.CTime.Format(time.RFC3339)
							fe.OS = fsLine.Meta.OS
							fe.Attr = fsLine.Meta.Attr
							break
						}
					}
				}
				if extIndexer != nil {
					fe.Pronom = extIndexer.Pronom
					fe.Mimetype = extIndexer.Mimetype
					fe.IdxSize = humanize.Bytes(extIndexer.Size)
				}
				files[name] = fe
				filenames = append(filenames, name)
			}
		}
	}

	var params = map[string]any{
		"title":     "Version",
		"id":        s.object.GetID(),
		"versions":  s.metadata.Versions,
		"files":     files,
		"filenames": filenames,
		"version":   iop.Version,
	}

	c.HTML(http.StatusOK, "version.gohtml", gin.H(params))

}

func (s *Server) loadObjectID(c *gin.Context) {
	var err error
	type idParam struct {
		ID string `uri:"id" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}
	if s.object != nil && s.object.GetID() == iop.ID {
		// already loaded
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID).Error()})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}
	s.displayObject(c)
}

func (s *Server) displayObjectBrowse(c *gin.Context) {
	path := c.Param("path")
	c.FileFromFS(path, s.httpObjectFS)

}
func (s *Server) loadObjectPath(c *gin.Context) {
	var err error
	type pathParam struct {
		Path string `uri:"path" binding:"required"`
	}
	var iop pathParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	folder := strings.Trim(iop.Path, "/")
	objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
	}
	s.objectFS = objectFS
	s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	defer s.object.Close()
	extractor := s.object.GetExtractor()
	s.metadata, err = extractor.GetMetadata()
	_ = extractor.Close()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
		return
	}
	c.Redirect(http.StatusPermanentRedirect, s.urlExt.String()+fmt.Sprintf("/object/id/%s", url.PathEscape(s.object.GetID())))
	//	s.displayObject(c)
}

type AreaStats struct {
	Name            string         `json:"name"`
	Path            string         `json:"path"`
	Description     string         `json:"description"`
	NumFiles        int            `json:"numFiles"`
	DifferentFiles  int            `json:"differentFiles"`
	Size            uint64         `json:"size"`
	SizeStr         string         `json:"sizeStr"`
	NoSizeFiles     int            `json:"noSizeFiles"`
	NoMimeTypeFiles int            `json:"noMimeTypeFiles"`
	NoPronomFiles   int            `json:"noPronomFiles"`
	MimeTypes       map[string]int `json:"mimeTypes"`
	Pronoms         map[string]int `json:"pronoms"`
}

type MimeCount struct {
	SizeStr string `json:"sizeStr"`
	Size    uint64 `json:"size"`
	Count   int    `json:"count"`
}

type flatEdge struct {
	Left  int
	Right int
	Name  string
}

type AreaReportStats struct {
	Name            string `json:"name"`
	Path            string `json:"path"`
	Description     string `json:"description"`
	NumFiles        int    `json:"numFiles"`
	DifferentFiles  int    `json:"differentFiles"`
	Size            uint64 `json:"size"`
	SizeStr         string `json:"sizeStr"`
	NoSizeFiles     int    `json:"noSizeFiles"`
	NoMimeTypeFiles int    `json:"noMimeTypeFiles"`
	NoPronomFiles   int    `json:"noPronomFiles"`
	AVLength        string `json:"avLength"`
	videoSecs       uint
	MimeTypes       map[string]*MimeCount `json:"mimeTypes"`
	Pronoms         map[string]*MimeCount `json:"pronoms"`
}

func extractSubPaths(mExtension any) map[string]ext_NNNN_content_subpath.ContentSubPathEntry {
	subPaths := make(map[string]ext_NNNN_content_subpath.ContentSubPathEntry)
	mExtensions, ok := mExtension.(map[string]any)
	if !ok {
		return subPaths
	}
	_subpathMeta, ok := mExtensions[ext_NNNN_content_subpath.ContentSubPathName]
	if !ok {
		return subPaths
	}
	if subPathMeta, ok := _subpathMeta.(map[string]ext_NNNN_content_subpath.ContentSubPathEntry); ok {
		for k, v := range subPathMeta {
			subPaths[k] = v
		}
	} else if subPathMap, ok := _subpathMeta.(map[string]any); ok {
		for k, v := range subPathMap {
			if entry, ok := v.(ext_NNNN_content_subpath.ContentSubPathEntry); ok {
				subPaths[k] = entry
			} else if entryMap, ok := v.(map[string]any); ok {
				var entry ext_NNNN_content_subpath.ContentSubPathEntry
				if p, ok := entryMap["path"].(string); ok {
					entry.Path = p
				}
				if d, ok := entryMap["description"].(string); ok {
					entry.Description = d
				}
				subPaths[k] = entry
			}
		}
	}
	return subPaths
}

func extractFileAreas(extension map[string]any) []string {
	var fileAreas []string
	if extension == nil {
		return []string{""}
	}
	if _subpath, ok := extension[ext_NNNN_content_subpath.ContentSubPathName]; ok {
		if fa, ok := _subpath.([]string); ok {
			fileAreas = append(fileAreas, fa...)
		} else if anyAreas, ok := _subpath.([]any); ok {
			for _, a := range anyAreas {
				if s, ok := a.(string); ok {
					fileAreas = append(fileAreas, s)
				}
			}
		} else if s, ok := _subpath.(string); ok {
			fileAreas = append(fileAreas, s)
		}
	}
	if len(fileAreas) == 0 {
		fileAreas = []string{""}
	}
	return fileAreas
}

func fileMatchesAreas(file *inventory.FileMetadata, reportAreas []string) bool {
	if len(reportAreas) == 0 {
		return true
	}
	fileAreas := extractFileAreas(file.Extension)
	for _, fa := range fileAreas {
		if slices.Contains(reportAreas, fa) {
			return true
		}
	}
	return false
}

func (s *Server) displayObject(c *gin.Context) {

	if s.metadata == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no metadata loaded"})
		return
	}
	var subPaths = extractSubPaths(s.metadata.Extension)

	var areaStatsMap = make(map[string]*AreaStats)
	for k, v := range subPaths {
		areaStatsMap[k] = &AreaStats{
			Name:        k,
			Path:        v.Path,
			Description: v.Description,
			MimeTypes:   make(map[string]int),
			Pronoms:     make(map[string]int),
		}
	}

	var numFiles int
	var size uint64
	var noSizeFiles int
	var noMimeTypeFiles int
	var noPronomFiles int
	var mimeTypes = make(map[string]int)
	var pronoms = make(map[string]int)
	for _, v := range s.metadata.Files {
		numFiles += len(v.InternalName)
		var fs map[string]any
		var idx *indexer.ResultV2
		var sizeDone bool
		var fileSize uint64
		var mimeType string
		var pronom string

		if _fs, ok := v.Extension[ext_NNNN_filesystem.FilesystemName]; ok {
			if fs, ok = _fs.(map[string]any); ok {
				if fs["size"] != nil {
					fileSize = fs["size"].(uint64)
					sizeDone = true
				}
			}
		}
		var areas = extractFileAreas(v.Extension)
		if _idx, ok := v.Extension[ext_NNNN_indexer.IndexerName]; ok {
			if idx, ok = _idx.(*indexer.ResultV2); ok {
				fileSize += idx.Size
				if idx.Size > 0 {
					sizeDone = true
				}
				mimeType = idx.Mimetype
				pronom = idx.Pronom
			}
		}
		if sizeDone {
			size += fileSize
		} else {
			noSizeFiles++
		}
		if mimeType != "" {
			if _, ok := mimeTypes[mimeType]; !ok {
				mimeTypes[mimeType] = 0
			}
			mimeTypes[mimeType]++
		} else {
			noMimeTypeFiles++
		}
		if pronom != "" {
			if _, ok := pronoms[pronom]; !ok {
				pronoms[pronom] = 0
			}
			pronoms[pronom]++
		} else {
			noPronomFiles++
		}

		for _, area := range areas {
			stat, ok := areaStatsMap[area]
			if !ok {
				entry := subPaths[area]
				desc := entry.Description
				if desc == "" && area == "" {
					desc = "Default Area"
				}
				stat = &AreaStats{
					Name:        area,
					Path:        entry.Path,
					Description: desc,
					MimeTypes:   make(map[string]int),
					Pronoms:     make(map[string]int),
				}
				areaStatsMap[area] = stat
			}
			stat.NumFiles += len(v.InternalName)
			stat.DifferentFiles++
			if sizeDone {
				stat.Size += fileSize
			} else {
				stat.NoSizeFiles++
			}
			if mimeType != "" {
				stat.MimeTypes[mimeType]++
			} else {
				stat.NoMimeTypeFiles++
			}
			if pronom != "" {
				stat.Pronoms[pronom]++
			} else {
				stat.NoPronomFiles++
			}
		}
	}

	var areaStatsList = make([]*AreaStats, 0, len(areaStatsMap))
	for _, stat := range areaStatsMap {
		if stat.NumFiles == 0 {
			continue
		}
		stat.SizeStr = humanize.Bytes(stat.Size)
		areaStatsList = append(areaStatsList, stat)
	}
	slices.SortFunc(areaStatsList, func(a, b *AreaStats) int {
		if a.Name == "" && b.Name != "" {
			return -1
		}
		if a.Name != "" && b.Name == "" {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})

	var id string
	if s.object != nil {
		id = s.object.GetID()
	} else if s.metadata != nil {
		id = s.metadata.ID
	}

	var params = map[string]any{
		"title":           "gocfl",
		"id":              id,
		"versions":        s.metadata.Versions,
		"differentFiles":  len(s.metadata.Files),
		"numFiles":        numFiles,
		"size":            humanize.Bytes(size),
		"noSizeFiles":     noSizeFiles,
		"noMimeTypeFiles": noMimeTypeFiles,
		"noPronomFiles":   noPronomFiles,
		"mimeTypes":       mimeTypes,
		"pronoms":         pronoms,
		"areaStats":       areaStatsList,
	}

	c.HTML(http.StatusOK, "object.gohtml", gin.H(params))
}

func (s *Server) loadObjectBrowser(c *gin.Context) {
	var err error
	type idParam struct {
		ID string `uri:"id" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}
	if s.object != nil && s.object.GetID() == iop.ID {
		// already loaded
		if s.metadata == nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
			if s.obfuscate {
				if err := s.metadata.Obfuscate(); err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot obfuscate metadata").Error()})
					return
				}
			}
		}
	} else {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID)})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
		if s.obfuscate {
			if err := s.metadata.Obfuscate(); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot obfuscate metadata").Error()})
				return
			}
		}
	}
	if s.httpObjectFS == nil {
		objectFS, err := NewObjectFS(s.object, s.objectFS)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get filesystem for object %s", s.object.GetID()).Error()})
			return
		}
		s.httpObjectFS = http.FS(objectFS)
	}
	s.displayObjectBrowse(c)
}

func (s *Server) report(c *gin.Context) {

	var err error
	type idParam struct {
		ID string `uri:"id" binding:"required"`
	}
	var iop idParam
	if err = c.ShouldBindUri(&iop); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	iop.ID, err = url.PathUnescape(iop.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot unescape '%s'", iop.ID).Error()})
		return
	}
	full := c.DefaultQuery("full", "none") != "none"
	not_polyfilled := c.DefaultQuery("polyfilled", "none") == "false"

	if (s.object != nil && s.object.GetID() == iop.ID) || (s.metadata != nil && s.metadata.ID == iop.ID) {
		if s.metadata == nil && s.object != nil {
			extractor := s.object.GetExtractor()
			s.metadata, err = extractor.GetMetadata()
			_ = extractor.Close()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
				return
			}
		}
	} else if s.storageRoot != nil {
		folder, err := s.storageRoot.IdToFolder(iop.ID)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get folder for object %s", iop.ID)})
			return
		}
		objectFS, err := writefs.Sub(s.storageRoot.GetReadFS(), folder)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot create subfs for %v / %s", s.storageRoot.GetReadFS(), folder)})
		}
		s.objectFS = objectFS
		s.object, err = ocfl.LoadObject(context.Background(), objectFS, nil, s.log)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer s.object.Close()
		extractor := s.object.GetExtractor()
		s.metadata, err = extractor.GetMetadata()
		_ = extractor.Close()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errors.Wrapf(err, "cannot get metadata for object %s", s.object.GetID()).Error()})
			return
		}
	}

	if s.metadata == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no metadata loaded"})
		return
	}

	var extManager objecttypes.ExtensionManager
	var inv inventory.Inventory
	if s.object != nil {
		extManager = s.object.GetExtensionManager()
		inv = s.object.GetInventory()
	}

	var subPaths = extractSubPaths(s.metadata.Extension)

	var areaStatsMap = make(map[string]*AreaReportStats)
	for k, v := range subPaths {
		areaStatsMap[k] = &AreaReportStats{
			Name:        k,
			Path:        v.Path,
			Description: v.Description,
			MimeTypes:   make(map[string]*MimeCount),
			Pronoms:     make(map[string]*MimeCount),
		}
	}

	var numFiles int
	var size uint64
	var noSizeFiles int
	var noMimeTypeFiles int
	var noPronomFiles int
	var mimeTypes = make(map[string]*MimeCount)
	var pronoms = make(map[string]*MimeCount)
	var videoSecs uint
	for _, v := range s.metadata.Files {
		numFiles += len(v.InternalName)
		_fs, _ := v.Extension[ext_NNNN_filesystem.FilesystemName]
		_idx, _ := v.Extension[ext_NNNN_indexer.IndexerName]
		var fs map[string]any
		var idx *indexer.ResultV2
		var ok bool
		var sizeDone bool
		var fileSize uint64
		var fileVideoSecs uint
		var mimeType string
		var pronom string

		if _fs != nil {
			if fs, ok = _fs.(map[string]any); ok {
				if fs["size"] != nil {
					fileSize = fs["size"].(uint64)
					sizeDone = true
				}
			}
		}
		var areas = extractFileAreas(v.Extension)
		if _idx != nil {
			if idx, ok = _idx.(*indexer.ResultV2); ok {
				fileSize += idx.Size
				fileVideoSecs = idx.Duration
				if idx.Size > 0 {
					sizeDone = true
				}
				mimeType = idx.Mimetype
				pronom = idx.Pronom
			}
		}
		if sizeDone {
			size += fileSize
		} else {
			noSizeFiles++
		}
		videoSecs += fileVideoSecs
		if mimeType != "" {
			if _, ok := mimeTypes[mimeType]; !ok {
				mimeTypes[mimeType] = &MimeCount{
					SizeStr: "",
					Size:    0,
					Count:   0,
				}
			}
			mimeTypes[mimeType].Count++
			if idx != nil {
				mimeTypes[mimeType].Size += idx.Size
			} else {
				mimeTypes[mimeType].Size += fileSize
			}
		} else {
			noMimeTypeFiles++
		}
		if pronom != "" {
			if _, ok := pronoms[pronom]; !ok {
				pronoms[pronom] = &MimeCount{
					SizeStr: "",
					Size:    0,
					Count:   0,
				}
			}
			pronoms[pronom].Count++
			if idx != nil {
				pronoms[pronom].Size += idx.Size
			} else {
				pronoms[pronom].Size += fileSize
			}
		} else {
			noPronomFiles++
		}

		for _, area := range areas {
			stat, ok := areaStatsMap[area]
			if !ok {
				entry := subPaths[area]
				desc := entry.Description
				if desc == "" && area == "" {
					desc = "Default Area"
				}
				stat = &AreaReportStats{
					Name:        area,
					Path:        entry.Path,
					Description: desc,
					MimeTypes:   make(map[string]*MimeCount),
					Pronoms:     make(map[string]*MimeCount),
				}
				areaStatsMap[area] = stat
			}
			stat.NumFiles += len(v.InternalName)
			stat.DifferentFiles++
			if sizeDone {
				stat.Size += fileSize
			} else {
				stat.NoSizeFiles++
			}
			stat.videoSecs += fileVideoSecs
			if mimeType != "" {
				if _, ok := stat.MimeTypes[mimeType]; !ok {
					stat.MimeTypes[mimeType] = &MimeCount{}
				}
				stat.MimeTypes[mimeType].Count++
				if idx != nil {
					stat.MimeTypes[mimeType].Size += idx.Size
				} else {
					stat.MimeTypes[mimeType].Size += fileSize
				}
			} else {
				stat.NoMimeTypeFiles++
			}
			if pronom != "" {
				if _, ok := stat.Pronoms[pronom]; !ok {
					stat.Pronoms[pronom] = &MimeCount{}
				}
				stat.Pronoms[pronom].Count++
				if idx != nil {
					stat.Pronoms[pronom].Size += idx.Size
				} else {
					stat.Pronoms[pronom].Size += fileSize
				}
			} else {
				stat.NoPronomFiles++
			}
		}
	}

	for _, pronomSize := range pronoms {
		pronomSize.SizeStr = humanize.Bytes(pronomSize.Size)
	}
	for _, mimeSize := range mimeTypes {
		mimeSize.SizeStr = humanize.Bytes(mimeSize.Size)
	}

	var areaStatsList = make([]*AreaReportStats, 0, len(areaStatsMap))
	for _, stat := range areaStatsMap {
		if stat.NumFiles == 0 {
			continue
		}
		stat.SizeStr = humanize.Bytes(stat.Size)
		stat.AVLength = fmtDuration(time.Duration(int64(stat.videoSecs) * int64(time.Second)))
		for _, pronomSize := range stat.Pronoms {
			pronomSize.SizeStr = humanize.Bytes(pronomSize.Size)
		}
		for _, mimeSize := range stat.MimeTypes {
			mimeSize.SizeStr = humanize.Bytes(mimeSize.Size)
		}
		areaStatsList = append(areaStatsList, stat)
	}
	slices.SortFunc(areaStatsList, func(a, b *AreaReportStats) int {
		if a.Name == "" && b.Name != "" {
			return -1
		}
		if a.Name != "" && b.Name == "" {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})

	var objectpath string
	if fsStringer, ok := s.objectFS.(fmt.Stringer); ok {
		objectpath = fsStringer.String()
	}

	var info = map[string]any{}
	if extManager != nil && s.objectFS != nil {
		cfg, err := extManager.GetConfigName(ext_NNNN_metafile.MetaFileName)
		if err != nil {
			cfg = &ext_NNNN_metafile.MetaFileConfig{
				ExtensionConfig: &extensiontypes.ExtensionConfig{ExtensionName: ext_NNNN_metafile.MetaFileName},
				StorageType:     "area",
				StorageName:     "metadata",
				MetaName:        "info.json",
				MetaSchema:      "none",
			}
		}

		metafileCfg, ok := cfg.(*ext_NNNN_metafile.MetaFileConfig)
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": errors.Errorf("invalid config format %v", cfg)})
			return
		}

		var infoBytes []byte
		if metafileCfg.StorageType == "extension" {
			fsys, err := writefs.Sub(s.objectFS, path.Join("extensions", ext_NNNN_metafile.MetaFileName))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			infoname := strings.TrimLeft(filepath.ToSlash(filepath.Join(metafileCfg.StorageName, metafileCfg.MetaName)), "/")
			infoBytes, err = fs.ReadFile(fsys, infoname)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": errors.Wrapf(err, "cannot open %v/%s", fsys, infoname).Error()})
				return
			}
		} else {
			area := "content"
			path := metafileCfg.StorageName
			if metafileCfg.StorageType == "area" {
				area = metafileCfg.StorageName
				path = ""
			}
			fname := filepath.ToSlash(filepath.Join(path, metafileCfg.MetaName))
			mPath, err := extManager.BuildObjectManifestPath(fname, area)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": errors.Wrapf(err, "cannot map %s:%s", area, fname).Error()})
				return
			}

			// search for info file
			for ver, _ := range s.metadata.Versions {
				fullpath := filepath.ToSlash(filepath.Join(ver, "content", mPath))
				jsonData, err := fs.ReadFile(s.objectFS, fullpath)
				if err == nil && len(jsonData) > 0 {
					infoBytes = jsonData
				}
			}
		}
		if len(infoBytes) > 0 {
			if err := json.Unmarshal(infoBytes, &info); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": errors.Wrapf(err, "cannot unmarshal %s", metafileCfg.MetaName).Error()})
				return
			}
		}
	}

	var filenames = []string{}

	type edge struct {
		indent   uint
		children []*edge
		parent   *edge
		name     string
	}

	var tree = &edge{
		indent:   0,
		children: []*edge{},
		parent:   nil,
		name:     "",
	}

	var maxDepth uint
	var addToTree func(parts []string, e *edge)
	addToTree = func(parts []string, e *edge) {
		if len(parts) == 0 {
			return
		}
		for _, child := range e.children {
			if child.name == parts[0] {
				addToTree(parts[1:], child)
				return
			}
		}
		newEdge := &edge{
			indent:   e.indent + 1,
			children: []*edge{},
			parent:   e,
			name:     parts[0],
		}
		if maxDepth <= e.indent {
			maxDepth = e.indent + 1
		}
		e.children = append(e.children, newEdge)
		addToTree(parts[1:], newEdge)
	}

	reportAreas := s.reportAreas
	if qAreas, ok := c.GetQueryArray("area"); ok && len(qAreas) > 0 {
		reportAreas = qAreas
	} else if qAreas, ok := c.GetQueryArray("reportarea"); ok && len(qAreas) > 0 {
		reportAreas = qAreas
	} else if qAreas, ok := c.GetQueryArray("reportareas"); ok && len(qAreas) > 0 {
		reportAreas = qAreas
	} else if qArea := c.Query("area"); qArea != "" {
		reportAreas = strings.Split(qArea, ",")
	} else if qArea := c.Query("reportarea"); qArea != "" {
		reportAreas = strings.Split(qArea, ",")
	} else if qArea := c.Query("reportareas"); qArea != "" {
		reportAreas = strings.Split(qArea, ",")
	}

	var cleanReportAreas []string
	for _, ra := range reportAreas {
		ra = strings.TrimSpace(ra)
		if ra != "" {
			cleanReportAreas = append(cleanReportAreas, ra)
		}
	}
	reportAreas = cleanReportAreas

	for _, file := range s.metadata.Files {
		if !fileMatchesAreas(file, reportAreas) {
			continue
		}
		for _, files := range file.VersionName {
			for _, filename := range files {
				filenames = append(filenames, filename)
				parts := strings.Split(filename, "/")
				if !full {
					parts = parts[0 : len(parts)-1]
				}
				addToTree(parts, tree)
			}
		}
	}

	var flatTree = []*flatEdge{}
	var flattenTree func(e *edge)
	flattenTree = func(e *edge) {
		if strings.TrimSpace(e.name) != "" {
			flatTree = append(flatTree, &flatEdge{
				Left:  int(e.indent),
				Right: int(maxDepth - e.indent),
				Name:  e.name,
			})
		}
		for _, child := range e.children {
			flattenTree(child)
		}
	}
	flattenTree(tree)

	var files = map[string]*inventory.FileMetadata{}
	if full {
		for cs, file := range s.metadata.Files {
			if fileMatchesAreas(file, reportAreas) {
				files[cs] = file
			}
		}
	}
	var filesNoData int64
	for _, file := range s.metadata.Files {
		if file.Extension[ext_NNNN_indexer.IndexerName] == nil && file.Extension[ext_NNNN_filesystem.FilesystemName] == nil {
			filesNoData++
		}
	}

	var head any
	if inv != nil {
		head = inv.GetHead()
	} else if s.metadata != nil {
		head = s.metadata.Head
	}
	var objID string
	if s.object != nil {
		objID = s.object.GetID()
	} else if s.metadata != nil {
		objID = s.metadata.ID
	}

	var params = map[string]any{
		"objectpath":      objectpath,
		"gocfl":           "gocfl",
		"head":            head,
		"id":              objID,
		"versions":        s.metadata.Versions,
		"differentFiles":  len(s.metadata.Files),
		"numFiles":        numFiles,
		"filesNoData":     filesNoData,
		"size":            size,
		"noSizeFiles":     noSizeFiles,
		"noMimeTypeFiles": noMimeTypeFiles,
		"noPronomFiles":   noPronomFiles,
		"mimeTypes":       mimeTypes,
		"pronoms":         pronoms,
		"areaStats":       areaStatsList,
		"reportAreas":     reportAreas,
		"files":           files,
		"info":            info,
		"avLength":        fmtDuration(time.Duration(int64(videoSecs) * int64(time.Second))),
		"tree":            flatTree,
		"full":            full,
		"polyfilled":      !not_polyfilled,
	}

	c.HTML(http.StatusOK, "report.gohtml", gin.H(params))
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%d:%02d:%02d", h, m, s)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return errors.WithStack(s.srv.Shutdown(ctx))
}

func (s *Server) GetHTTPAddr() string {
	return s.HTTPAddr
}

func (s *Server) GetAddr() string {
	return s.HTTPAddr
}
