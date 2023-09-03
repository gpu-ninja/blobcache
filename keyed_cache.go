/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2009 The Go Authors. All rights reserved.
 * Copyright (c) 2023 Damian Peckett <damian@pecke.tt>.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *   * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *   * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package blobcache

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"
)

type KeyedEntry struct {
	OutputID ID
	Size     int64
	Time     time.Time
}

// KeyedCache is a cache that stores blobs by a key.
type KeyedCache struct {
	logger   *zap.Logger
	dir      string
	newHash  func() hash.Hash
	hashSize int64
	now      func() time.Time // For testing.
}

func NewKeyedCache(logger *zap.Logger, dir string, newHash func() hash.Hash, hashSize int64, now func() time.Time) (*KeyedCache, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, &os.PathError{Op: "open", Path: dir, Err: fmt.Errorf("not a directory")}
	}

	for i := 0; i < 256; i++ {
		name := filepath.Join(dir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(name, 0o777); err != nil {
			return nil, err
		}
	}

	if now == nil {
		now = time.Now
	}

	return &KeyedCache{
		logger:   logger,
		dir:      dir,
		newHash:  newHash,
		hashSize: hashSize,
		now:      now,
	}, nil
}

// Get looks up the ID in the cache and returns a reader if found.
func (c *KeyedCache) Get(id ID) (file io.ReadSeekCloser, entry KeyedEntry, err error) {
	entry, err = c.getIndexEntry(id)
	if err != nil {
		return nil, KeyedEntry{}, err
	}

	if err := c.used(c.fileName(id, "a")); err != nil {
		return nil, KeyedEntry{}, err
	}

	path, err := c.outputFile(entry.OutputID)
	if err != nil {
		return nil, KeyedEntry{}, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, KeyedEntry{}, err
	}

	if info.Size() != entry.Size {
		return nil, KeyedEntry{}, &os.PathError{Op: "stat", Path: path, Err: errors.New("file incomplete")}
	}

	file, err = os.Open(path)
	if err != nil {
		return nil, KeyedEntry{}, err
	}

	return file, entry, nil
}

func (c *KeyedCache) getIndexEntry(id ID) (KeyedEntry, error) {
	// action entry file is "v1 <hex id> <hex out> <decimal size space-padded to 20 bytes> <unixnano space-padded to 20 bytes>\n"
	hexSize := int(c.hashSize * 2)
	entrySize := 2 + 1 + hexSize + 1 + hexSize + 1 + 20 + 1 + 20 + 1

	missing := func(reason error) (KeyedEntry, error) {
		return KeyedEntry{}, &os.PathError{Op: "get", Path: hex.EncodeToString(id),
			Err: fmt.Errorf("%v: %w", reason, os.ErrNotExist)}
	}
	f, err := os.Open(c.fileName(id, "a"))
	if err != nil {
		return missing(err)
	}
	defer f.Close()
	entry := make([]byte, entrySize+1) // +1 to detect whether f is too long
	if n, err := io.ReadFull(f, entry); n > entrySize {
		return missing(errors.New("too long"))
	} else if err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			return missing(errors.New("file is empty"))
		}
		return missing(err)
	} else if n < entrySize {
		return missing(errors.New("entry file incomplete"))
	}
	if entry[0] != 'v' || entry[1] != '1' || entry[2] != ' ' || entry[3+hexSize] != ' ' || entry[3+hexSize+1+hexSize] != ' ' || entry[3+hexSize+1+hexSize+1+20] != ' ' || entry[entrySize-1] != '\n' {
		return missing(errors.New("invalid header"))
	}
	eid, entry := entry[3:3+hexSize], entry[3+hexSize:]
	eout, entry := entry[1:1+hexSize], entry[1+hexSize:]
	esize, entry := entry[1:1+20], entry[1+20:]
	etime := entry[1 : 1+20]
	buf := make([]byte, c.hashSize)
	if _, err := hex.Decode(buf, eid); err != nil {
		return missing(fmt.Errorf("decoding ID: %v", err))
	} else if !bytes.Equal(buf, id) {
		return missing(errors.New("mismatched ID"))
	}
	if _, err := hex.Decode(buf, eout); err != nil {
		return missing(fmt.Errorf("decoding output ID: %v", err))
	}
	i := 0
	for i < len(esize) && esize[i] == ' ' {
		i++
	}
	size, err := strconv.ParseInt(string(esize[i:]), 10, 64)
	if err != nil {
		return missing(fmt.Errorf("parsing size: %v", err))
	} else if size < 0 {
		return missing(errors.New("negative size"))
	}
	i = 0
	for i < len(etime) && etime[i] == ' ' {
		i++
	}
	tm, err := strconv.ParseInt(string(etime[i:]), 10, 64)
	if err != nil {
		return missing(fmt.Errorf("parsing timestamp: %v", err))
	} else if tm < 0 {
		return missing(errors.New("negative timestamp"))
	}

	return KeyedEntry{
		OutputID: buf,
		Size:     size,
		Time:     time.Unix(0, tm),
	}, nil
}

// Put stores the given output in the cache as the output for the action ID.
// It may read file twice. The content of file must not change between the two passes.
func (c *KeyedCache) Put(id ID, file io.ReadSeeker) (ID, int64, error) {
	h := c.newHash()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ID{}, 0, err
	}

	size, err := io.Copy(h, file)
	if err != nil {
		return ID{}, 0, err
	}

	out := h.Sum(nil)

	// Copy to cached output file (if not already present).
	if err := c.copyFile(file, out, size); err != nil {
		return out, size, err
	}

	// Add to cache index.
	return out, size, c.putIndexEntry(id, out, size)
}

// putIndexEntry adds an entry to the cache recording that executing the action
// with the given id produces an output with the given output id (hash) and size.
func (c *KeyedCache) putIndexEntry(id ID, out ID, size int64) error {
	entry := fmt.Sprintf("v1 %x %x %20d %20d\n", id, out, size, time.Now().UnixNano())
	file := c.fileName(id, "a")

	// Copy file to cache directory.
	mode := os.O_WRONLY | os.O_CREATE
	f, err := os.OpenFile(file, mode, 0666)
	if err != nil {
		return err
	}

	_, err = f.WriteString(entry)
	if err == nil {
		// Truncate the file only *after* writing it.
		// (This should be a no-op, but truncate just in case of previous corruptio
		err = f.Truncate(int64(len(entry)))
	}

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(file)
		return err
	}

	return os.Chtimes(file, c.now(), c.now()) // mainly for tests
}

// Size returns the total size of the cache in bytes.
func (c *KeyedCache) Size() (int64, error) {
	var size int64
	err := filepath.Walk(c.dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, "-a") {
			eid := strings.TrimSuffix(filepath.Base(path), "-a")

			id := make(ID, c.hashSize)
			if _, err := hex.Decode(id, []byte(eid)); err != nil {
				return err
			}

			entry, err := c.getIndexEntry(id)
			if err != nil {
				c.logger.Warn("Failed to get index entry",
					zap.String("id", eid), zap.Error(err))
			} else {
				size += entry.Size
			}
		}

		return nil
	})

	return size, err
}

// Trim removes old cache entries that are likely not to be reused.
func (c *KeyedCache) Trim(maxBytes int64) error {
	const maxIterations = 20

	maxAge := trimLimit
	now := c.now()

	for i := 0; i < maxIterations; i++ {
		c.logger.Debug("Trimming cache", zap.Stringer("maxAge", maxAge))

		cutoff := now.Add(-maxAge)
		for i := 0; i < 256; i++ {
			subdir := filepath.Join(c.dir, fmt.Sprintf("%02x", i))
			if err := c.trimSubdir(subdir, cutoff); err != nil {
				c.logger.Warn("Failed to trim subdirectory",
					zap.String("subdir", subdir), zap.Error(err))
			}
		}

		if maxBytes == 0 {
			return nil
		}

		size, err := c.Size()
		if err != nil {
			return err
		}

		c.logger.Debug("Trimmed cache size",
			zap.String("size", units.BytesSize(float64(size))))

		// If we're still over the size limit, trim more.
		if size > maxBytes {
			maxAge /= 2
		} else {
			return nil
		}
	}

	return fmt.Errorf("exceeded max iterations")
}

// trimSubdir trims a single cache subdirectory.
func (c *KeyedCache) trimSubdir(subdir string, cutoff time.Time) error {
	// Read all directory entries from subdir before removing
	// any files, in case removing files invalidates the file offset
	// in the directory scan. Also, ignore error from f.Readdirnames,
	// because we don't care about reporting the error and we still
	// want to process any entries found before the error.
	f, err := os.Open(subdir)
	if err != nil {
		return err
	}

	names, err := f.Readdirnames(-1)
	_ = f.Close()
	if err != nil {
		return err
	}

	for _, name := range names {
		// Remove only cache entries (xxxx-a and xxxx-d).
		if !strings.HasSuffix(name, "-a") && !strings.HasSuffix(name, "-d") {
			continue
		}
		entry := filepath.Join(subdir, name)
		info, err := os.Stat(entry)
		if err == nil && info.ModTime().Before(cutoff) {
			c.logger.Debug("Removing old cache entry", zap.String("entry", entry))

			os.Remove(entry)
		}
	}

	return nil
}

// copyFile copies file into the cache, expecting it to have the given
// output ID and size, if that file is not present already.
func (c *KeyedCache) copyFile(file io.ReadSeeker, out ID, size int64) error {
	name := c.fileName(out, "d")
	info, err := os.Stat(name)
	if err == nil && info.Size() == size {
		// Check hash.
		if f, err := os.Open(name); err == nil {
			h := c.newHash()
			if _, err := io.Copy(h, f); err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}

			out2 := h.Sum(nil)
			if bytes.Equal(out, out2) {
				return nil
			}
		}
		// Hash did not match. Fall through and rewrite file.
	}

	// Copy file to cache directory.
	mode := os.O_RDWR | os.O_CREATE
	if err == nil && info.Size() > size { // shouldn't happen but fix in case
		mode |= os.O_TRUNC
	}
	f, err := os.OpenFile(name, mode, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	if size == 0 {
		// File now exists with correct size.
		// Only one possible zero-length file, so contents are OK too.
		// Early return here makes sure there's a "last byte" for code below.
		return nil
	}

	// From here on, if any of the I/O writing the file fails,
	// we make a best-effort attempt to truncate the file f
	// before returning, to avoid leaving bad bytes in the file.

	// Copy file to f, but also into h to double-check hash.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = f.Truncate(0)
		return err
	}

	h := c.newHash()
	w := io.MultiWriter(f, h)

	if _, err := io.CopyN(w, file, size-1); err != nil {
		_ = f.Truncate(0)
		return err
	}
	// Check last byte before writing it; writing it will make the size match
	// what other processes expect to find and might cause them to start
	// using the file.
	buf := make([]byte, 1)
	if _, err := file.Read(buf); err != nil {
		_ = f.Truncate(0)
		return err
	}

	_, _ = h.Write(buf)

	sum := h.Sum(nil)
	if !bytes.Equal(sum, out) {
		_ = f.Truncate(0)
		return fmt.Errorf("file content changed underfoot")
	}

	// Commit cache file entry.
	if _, err := f.Write(buf); err != nil {
		_ = f.Truncate(0)
		return err
	}
	if err := f.Close(); err != nil {
		// Data might not have been written,
		// but file may look like it is the right size.
		// To be extra careful, remove cached file.
		os.Remove(name)
		return err
	}

	return os.Chtimes(name, c.now(), c.now()) // mainly for tests
}

// outputFile returns the name of the cache file storing output with the given ID.
func (c *KeyedCache) outputFile(out ID) (string, error) {
	file := c.fileName(out, "d")
	return file, c.used(file)
}

// fileName returns the name of the file corresponding to the given id.
func (c *KeyedCache) fileName(id ID, key string) string {
	return filepath.Join(c.dir, fmt.Sprintf("%02x", id[0]), fmt.Sprintf("%x", id)+"-"+key)
}

// used makes a best-effort attempt to update mtime on file,
// so that mtime reflects cache access time.
func (c *KeyedCache) used(file string) error {
	info, err := os.Stat(file)
	if err == nil && c.now().Sub(info.ModTime()) < mtimeInterval {
		return nil
	}

	return os.Chtimes(file, c.now(), c.now())
}
