package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/keepass2kms"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/osfsrw"
	"github.com/ocfl-archive/filesystem/pkg/s3fsrw"
	"github.com/ocfl-archive/filesystem/pkg/vfsrw"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/filesystem/pkg/zipfs"
	"github.com/ocfl-archive/filesystem/pkg/zipfsrw"
	"github.com/ocfl-archive/gocfl-cli/config"
	"github.com/ocfl-archive/gocfl-cli/internal"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/spf13/cobra"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

func setupLogger(ctx context.Context, ver version.OCFLVersion) (ocfllogger.OCFLLogger, []io.Closer, error) {
	var closers []io.Closer
	hostname, err := os.Hostname()
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot get hostname")
	}

	var loggerTLSConfig *tls.Config
	if conf.Log.Stash.TLS != nil {
		var loggerLoader io.Closer
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			return nil, nil, errors.Wrap(err, "cannot create client loader")
		}
		closers = append(closers, loggerLoader)
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create logger")
	}
	if _logstash != nil {
		closers = append(closers, _logstash)
	}
	if _logfile != nil {
		closers = append(closers, _logfile)
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger()
	return ocfllogger.NewOCFLLogger(ctx, &l2, nil, ver, nil), closers, nil
}

func setupVFS(logger ocfllogger.OCFLLogger) (vfsrw.VFSRW, error) {
	if conf.VFS == nil {
		conf.VFS = vfsrw.Config{}
	}
	for name, val := range getLocalFSConfig() {
		conf.VFS[name] = val
	}
	vfs, err := vfsrw.NewFS(conf.VFS, logger.Logger())
	if err != nil {
		return nil, errors.Wrap(err, "cannot create VFS")
	}
	if err := vfsrw.AddLocal(vfs, &vfsrw.ZipAsFolder{
		Enabled:   true,
		Digests:   []checksum.DigestAlgorithm{checksum.DigestSHA512},
		CacheSize: 3,
		Compress:  false,
		ReadOnly:  false,
	}); err != nil {
		return nil, errors.Wrap(err, "cannot add local VFS")
	}
	vfs.AddFS("internal", nil, internal.InternalFS)
	return vfs, nil
}

func firstOrSecond[T any](first bool, a T, b T) T {
	if first {
		return a
	}
	return b
}

type AddFS interface {
	AddFS(name string, fsys fs.FS)
}

func getLocalFSConfig() map[string]*vfsrw.VFS {
	var result = map[string]*vfsrw.VFS{}
	if runtime.GOOS == "windows" {
		partitions, _ := disk.Partitions(false)
		for _, partition := range partitions {
			if len(partition.Mountpoint) < 2 {
				continue
			}
			if partition.Mountpoint[1] != ':' {
				continue
			}
			result[strings.ToLower(partition.Mountpoint[:1])] = &vfsrw.VFS{
				Name:     strings.ToLower(partition.Mountpoint[:1]),
				Type:     "os",
				ReadOnly: false,
				ZipAsFolder: &vfsrw.ZipAsFolder{
					Enabled:   true,
					CacheSize: 2,
					Compress:  false,
				},
				OS: &vfsrw.OS{
					BaseDir: partition.Mountpoint + "/",
				},
			}
		}
	} else {
		result["root"] = &vfsrw.VFS{
			Name:     "root",
			Type:     "os",
			ReadOnly: false,
			ZipAsFolder: &vfsrw.ZipAsFolder{
				Enabled:   true,
				CacheSize: 2,
				Compress:  false,
			},
			OS: &vfsrw.OS{
				BaseDir: "/",
			},
		}

	}
	return result
}

func resolveExtensionParam(cmd *cobra.Command, name, extensionName, param, defaultValue string) string {
	configValue := conf.Extension[extensionName][param]
	flagValue, _ := cmd.Flags().GetString(name)

	if configValue == "" {
		return flagValue
	}
	if flagValue != "" && flagValue != defaultValue {
		return flagValue
	}
	return configValue
}

func getExtensionParams(cmd *cobra.Command) (map[string]string, error) {
	var extensionParams = map[string]string{}
	if err := extension.GetExtensionParamValues(cmd.Name(), func(name, extensionName, param, defaultValue string) {
		if name == "" {
			return
		}
		extensionParams[name] = resolveExtensionParam(cmd, name, extensionName, param, defaultValue)
	}); err != nil {
		return nil, errors.Wrap(err, "cannot get extension params")
	}
	return extensionParams, nil
}

func startTimer() *timer {
	t := &timer{}
	t.Start()
	return t
}

type timer struct {
	start time.Time
}

func (t *timer) Start() {
	t.start = time.Now()
}

func (t *timer) String() string {
	delta := time.Since(t.start)
	return delta.String()
}

func path2vfs(pathStr string) (string, error) {
	pathStr = filepath.ToSlash(pathStr)
	if runtime.GOOS == "windows" {
		if len(pathStr) > 2 && pathStr[1] == ':' {
			pathStr = "vfs://" + path.Join(strings.ToLower(pathStr[:1]), pathStr[2:])
		} else {
			wd, err := os.Getwd()
			if err != nil {
				return "", errors.Wrap(err, "getting working directory")
			}
			pathStr = path.Join(filepath.ToSlash(wd), pathStr)
			pathStr = "vfs://" + path.Join(strings.ToLower(pathStr[:1]), pathStr[2:])
		}
	} else {
		if pathStr[0] != '/' {
			wd, err := os.Getwd()
			if err != nil {
				return "", errors.Wrap(err, "getting working directory")
			}
			pathStr = path.Join(wd, pathStr)
		}
		pathStr = "vfs://root" + pathStr
	}
	return pathStr, nil
}

// todo: use filesystem VFS
func initializeFSFactory(zipDigests []checksum.DigestAlgorithm, aesConfig *config.AESConfig, s3Config *config.S3Config, noCompression, readOnly bool, logger ocfllogger.OCFLLogger) (*writefs.Factory, error) {
	if zipDigests == nil {
		zipDigests = []checksum.DigestAlgorithm{checksum.DigestSHA512}
	}
	if aesConfig == nil {
		aesConfig = &config.AESConfig{}
	}
	if s3Config == nil {
		s3Config = &config.S3Config{}
	}

	fsFactory, err := writefs.NewFactory()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create filesystem factory")
	}

	if readOnly {
		if err := fsFactory.Register(zipfs.NewCreateFSFunc(logger.Logger()), "\\.zip$", writefs.HighFS); err != nil {
			return nil, errors.Wrap(err, "cannot register zipfs")
		}
	} else {
		// todo: allow different KMS clients
		if aesConfig.Enable {
			db, err := keepass2kms.LoadKeePassDBFromFile(string(aesConfig.KeepassFile), string(aesConfig.KeepassKey))
			if err != nil {
				return nil, errors.Wrapf(err, "cannot load keepass file '%s'", aesConfig.KeepassFile)
			}
			client, err := keepass2kms.NewClient(db, filepath.Base(string(aesConfig.KeepassFile)))
			if err != nil {
				return nil, errors.Wrap(err, "cannot create keepass2kms client")
			}
			registry.RegisterKMSClient(client)
			// todo: check for existence of key

			if err := fsFactory.Register(zipfsrw.NewCreateFSEncryptedChecksumFunc(noCompression, zipDigests, string(aesConfig.KeepassEntry), logger.Logger()), "\\.zip$", writefs.HighFS); err != nil {
				return nil, errors.Wrap(err, "cannot register FSEncryptedChecksum")
			}
		} else {
			if err := fsFactory.Register(zipfsrw.NewCreateFSChecksumFunc(noCompression, zipDigests, logger.Logger()), "\\.zip$", writefs.HighFS); err != nil {
				return nil, errors.Wrap(err, "cannot register FSChecksum")
			}
		}
	}
	if err := fsFactory.Register(osfsrw.NewCreateFSFunc(logger.Logger()), "", writefs.LowFS); err != nil {
		return nil, errors.Wrap(err, "cannot register osfs")
	}
	if s3Config.Endpoint != "" {
		if err := fsFactory.Register(
			s3fsrw.NewCreateFSFunc(
				map[string]*s3fsrw.S3Access{
					"switch": {
						string(s3Config.AccessKeyID),
						string(s3Config.AccessKey),
						string(s3Config.Endpoint),
						true,
					},
				},
				s3fsrw.ARNRegexStr,
				false,
				nil,
				"",
				"",
				logger.Logger(),
			),
			s3fsrw.ARNRegexStr,
			writefs.MediumFS,
		); err != nil {
			return nil, errors.Wrap(err, "cannot register s3fs")
		}
	}
	return fsFactory, nil
}

func showStatus(logger ocfllogger.OCFLLogger) error {
	contextString := ""
	errs := 0
	for _, err := range logger.ValidationErrors() {
		if err.Code[0] == 'E' {
			errs++
		}
		if err.Context != contextString {
			fmt.Printf("\n[%s]\n", err.Context)
			contextString = err.Context
		}
		fmt.Printf("   #%s - %s [%s]\n", err.Code, err.Description, err.Description2)
		//logger.Info().Msgf("ERROR: %v", err)
	}
	if errs > 0 {
		fmt.Printf("\n%d errors found\n", errs)
	} else {
		fmt.Printf("\nno errors found\n")
	}
	return nil
}

func addObjectByPath(
	ctx context.Context,
	sr storageroot.StorageRoot,
	fixity []checksum.DigestAlgorithm,
	extensionParams map[string]string,
	checkDuplicates bool,
	id, userName, userAddress, message string,
	sourceFS fs.FS,
	extensionFS fs.FS,
	area string,
	areaPaths map[string]fs.FS,
	echo bool,
	logger ocfllogger.OCFLLogger,
) (bool, error) {
	if fixity == nil {
		fixity = []checksum.DigestAlgorithm{}
	}
	var o object.Object
	objPath, err := sr.IdToFolder(id)
	if err != nil {
		return false, errors.Wrapf(err, "cannot create folder for id %s", id)
	}
	objectFS, err := appendfs.Sub(sr.GetWriteFS(), objPath)
	if err != nil {
		return false, errors.Wrapf(err, "cannot create subfs %v / %s for id %s", sr.GetWriteFS(), objPath, id)
	}
	exists, err := sr.ObjectExists(id)
	if err != nil {
		return false, errors.Wrapf(err, "cannot check for existence of %s", id)
	}
	if exists {
		var objCloser io.Closer
		o, objCloser, err = initocfl.LoadObject(ctx, objectFS, extensionParams, logger)
		if err != nil {
			return false, errors.Wrapf(err, "cannot load object %s", id)
		}
		defer objCloser.Close()
		// if we update, fixity is taken from last object version
		f := o.GetInventory().GetFixity()
		for alg := range f.GetDigestAlgorithms() {
			fixity = append(fixity, alg)
		}
	} else {
		if extensionParams == nil {
			return false, errors.New("extension manager is nil")
		}
		o, err = initocfl.InitObject(ctx, objectFS, extensionFS, sr.GetOCFLVersion(), id, sr.GetDigest(), extensionParams, logger)
		if err != nil {
			return false, errors.Wrapf(err, "cannot create object %s", id)
		}
	}
	versionWriter, err := o.StartUpdate(message, userName, userAddress, echo)
	if err != nil {
		return false, errors.Wrapf(err, "cannot start update for object %s", id)
	}
	defer func() {
		if versionWriter != nil {
			if err := versionWriter.Close(); err != nil {
				logger.Error().Err(err).Msg("cannot close version writer")
			}
		}
	}()
	if err := versionWriter.AddFolder(sourceFS, checkDuplicates, area); err != nil {
		return false, errors.Wrapf(err, "cannot add folder '%s' to '%s'", sourceFS, id)
	}
	if areaPaths != nil {
		for a, aPath := range areaPaths {
			if err := versionWriter.AddFolder(aPath, checkDuplicates, a); err != nil {
				return false, errors.Wrapf(err, "cannot add area '%s' folder '%s' to '%s'", a, aPath, id)
			}
		}
	}
	if err := versionWriter.Close(); err != nil {
		versionWriter = nil
		return false, errors.Wrapf(err, "cannot close version writer for object %s", id)
	}
	versionWriter = nil
	return o.GetInventory().IsModified(), nil
}

func CreateStorageRoot(ctx context.Context, objectWriteFS appendfs.FS, extensionConfigFS fs.FS, ver version.OCFLVersion, digest checksum.DigestAlgorithm, params map[string]string, logger ocfllogger.OCFLLogger) (storageroot.StorageRoot, error) {
	sr, err := initocfl.InitStorageRoot(ctx, objectWriteFS, extensionConfigFS, ver, digest, params, logger)
	if err != nil {
		return nil, err
	}
	return sr, nil
}

func LoadStorageRoot(
	ctx context.Context,
	storageRootFS appendfs.FS,
	extensionFactory extension.Factory[storageroot.ExtensionManager],
	logger ocfllogger.OCFLLogger,
) (storageroot.StorageRoot, io.Closer, error) {
	return initocfl.LoadStorageRoot(ctx, storageRootFS, nil, logger)
}

func LoadStorageRootRO(
	ctx context.Context,
	storageRootFS fs.FS,
	extensionFactory extension.Factory[storageroot.ExtensionManager],
	logger ocfllogger.OCFLLogger,
) (storageroot.StorageRoot, io.Closer, error) {
	return initocfl.LoadStorageRoot(ctx, storageRootFS, nil, logger)
}
