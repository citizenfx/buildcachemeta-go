package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iafan/cwalk"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ContentFile struct {
	Name           string `xml:"Name,attr"`
	Size           int64  `xml:"Size,attr"`
	SHA1Hash       string `xml:"SHA1Hash,attr"`
	SHA256Hash     string `xml:"SHA256Hash,attr,omitempty"`
	CompressedSize int64  `xml:"CompressedSize,attr"`
}

type CacheInfo struct {
	ContentFiles []*ContentFile `xml:"ContentFile,allowempty"`
}

func (cf *CacheInfo) Serialize() (string, error) {
	bytes, err := xml.MarshalIndent(cf, "", "\t")

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	sourcePath := flag.String("source", ".", "source of cache")
	bucketEndpoint := flag.String("s3-endpoint", "", "s3 endpoint")
	bucketKeyID := flag.String("s3-key-id", "", "s3 key id")
	bucketKey := flag.String("s3-key", "", "s3 key")
	bucketName := flag.String("s3-name", "updates", "s3 name")
	branchName := flag.String("branch-name", "dummy", "branch name")
	branchVersion := flag.String("branch-version", "dummy", "branch version")
	cacheName := flag.String("cache-name", "fivereborn", "cache name")
	bootstrapExe := flag.String("bootstrap-executable", "CitizenFX.exe", "current bootstrap executable")
	bootstrapVersion := flag.String("bootstrap-version", "10000000", "current bootstrap version")
	flag.Parse()

	bootstrapSize := int64(0)
	bootstrapHash := ""

	cxt := context.Background()

	minioOptions := &minio.Options{
		Creds:  credentials.NewStaticV4(*bucketKeyID, *bucketKey, ""),
		Secure: true,
	}

	minioClient, err := minio.New(*bucketEndpoint, minioOptions)

	if err != nil {
		return err
	}

	files := make(chan *ContentFile, 8192)

	cwalk.NumWorkers = 64

	go func() {
		err := cwalk.Walk(*sourcePath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			if filepath.Ext(path) == ".xz" {
				return nil
			}

			fpath := filepath.Join(*sourcePath, path)
			f, err := os.Open(fpath)

			if err != nil {
				return err
			}

			defer f.Close()

			h1 := sha1.New()
			if _, err := io.Copy(h1, f); err != nil {
				return err
			}

			f.Seek(0, io.SeekStart)

			h2 := sha256.New()
			if _, err := io.Copy(h2, f); err != nil {
				return err
			}

			f.Close()

			npath := strings.ReplaceAll(path, "\\", "/")

			s256 := h2.Sum(nil)

			exists := false
			compressed := false

			if info.Size() > 8192 {
				compressed = true
			}

			suffix := ""

			if compressed {
				suffix = ".xz"
			}

			objectKey := fmt.Sprintf("%x/%x/%x%v", s256[0:1], s256[1:2], s256, suffix)

			remoteInfo, err := minioClient.StatObject(cxt, *bucketName, objectKey, minio.StatObjectOptions{})

			if err == nil {
				exists = true
			}

			cSize := info.Size()

			if exists {
				cSize = remoteInfo.Size
			}

			if !exists {
				spath := fpath

				if compressed {
					cmd := exec.Command("xz", "-zfk", "-T", "0", fpath)
					err = cmd.Start()

					if err != nil {
						return err
					}

					err = cmd.Wait()

					if err != nil {
						return err
					}

					cstat, err := os.Stat(fpath + ".xz")

					if err != nil {
						return err
					}

					cSize = cstat.Size()
					spath += ".xz"
				}

				// function to let defer work
				err = (func() error {
					f, err = os.Open(spath)

					if err != nil {
						return err
					}

					defer f.Close()

					// upload the object
					_, err = minioClient.PutObject(cxt, *bucketName, objectKey, f, cSize, minio.PutObjectOptions{
						ContentType: "application/octet-stream",
						UserMetadata: map[string]string{
							"FileName": strings.ReplaceAll(path, "\\", "/"),
						},
					})

					return err
				})()

				if err != nil {
					return err
				}

				// remove any .xz we just made
				os.Remove(fpath + ".xz")

				fmt.Printf("%v -> %v...\n", fpath, objectKey)
			}

			if bootstrapExe != nil && path == *bootstrapExe {
				bootstrapHash = fmt.Sprintf("%X", s256)
				bootstrapSize = cSize
			} else {
				files <- &ContentFile{
					Name:           npath,
					Size:           info.Size(),
					SHA1Hash:       fmt.Sprintf("%X", h1.Sum(nil)),
					SHA256Hash:     fmt.Sprintf("%X", s256),
					CompressedSize: cSize,
				}
			}

			return nil
		})

		close(files)

		if err != nil {
			panic(err)
		}
	}()

	infoFile := &CacheInfo{}

	for file := range files {
		infoFile.ContentFiles = append(infoFile.ContentFiles, file)
	}

	sort.Slice(infoFile.ContentFiles, func(i, j int) bool {
		return infoFile.ContentFiles[i].Name < infoFile.ContentFiles[j].Name
	})

	xmlData, err := infoFile.Serialize()

	if err != nil {
		return err
	}

	var manifestHash string

	{
		h2 := sha256.New()
		if _, err := h2.Write([]byte(xmlData)); err != nil {
			return err
		}

		s256 := h2.Sum(nil)
		manifestHash = fmt.Sprintf("%x", s256)
		f := bytes.NewReader([]byte(xmlData))

		objectKey := fmt.Sprintf("%x/%x/%x", s256[0:1], s256[1:2], s256)

		_, err = minioClient.PutObject(cxt, *bucketName, objectKey, f, -1, minio.PutObjectOptions{
			ContentType: "application/octet-stream",
			UserMetadata: map[string]string{
				"FileName": "info.xml",
			},
		})

		if err != nil {
			return err
		}
	}

	{
		_, err = minioClient.PutObject(cxt, *bucketName, fmt.Sprintf("tags/%v/%v/%v", *cacheName, *branchName, *branchVersion), bytes.NewReader([]byte(*branchVersion)), -1, minio.PutObjectOptions{
			UserMetadata: map[string]string{
				"branch-version":    *branchVersion,
				"branch-manifest":   manifestHash,
				"bootstrap-version": *bootstrapVersion,
				"bootstrap-size":    fmt.Sprintf("%v", bootstrapSize),
				"bootstrap-object":  bootstrapHash,
			},
		})

		if err != nil {
			return err
		}
	}

	{
		_, err = minioClient.PutObject(cxt, *bucketName, fmt.Sprintf("heads/%v/%v", *cacheName, *branchName), bytes.NewReader([]byte(*branchVersion)), -1, minio.PutObjectOptions{
			UserMetadata: map[string]string{
				"branch-version":    *branchVersion,
				"branch-manifest":   manifestHash,
				"bootstrap-version": *bootstrapVersion,
				"bootstrap-size":    fmt.Sprintf("%v", bootstrapSize),
				"bootstrap-object":  bootstrapHash,
			},
		})

		if err != nil {
			return err
		}
	}

	return nil
}
