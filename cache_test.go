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

package blobcache_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/gpu-ninja/blobcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestCache(t *testing.T) {
	logger := zaptest.NewLogger(t)

	blob := make([]byte, 1000000)
	_, err := io.ReadFull(rand.Reader, blob)
	require.NoError(t, err)

	cacheDir := t.TempDir()

	c, err := blobcache.NewCache(logger, cacheDir, sha256.New, sha256.Size, func() time.Time {
		return time.Now().Add(-5 * time.Hour)
	})
	require.NoError(t, err)

	id, size, err := c.Put(bytes.NewReader(blob))
	require.NoError(t, err)

	assert.Equal(t, len(id), sha256.Size)
	assert.Equal(t, int64(len(blob)), size)

	// Should be a no-op.
	_, _, err = c.Put(bytes.NewReader(blob))
	require.NoError(t, err)

	file, e, err := c.Get(id)
	require.NoError(t, err)

	assert.NotEmpty(t, file)
	assert.Equal(t, int64(len(blob)), e.Size)
	assert.NotZero(t, e.Time)

	readBlob, err := io.ReadAll(file)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	assert.Equal(t, blob, readBlob)

	totalSize, err := c.Size()
	require.NoError(t, err)

	assert.Equal(t, int64(len(blob)), totalSize)

	err = c.Trim(0)
	require.NoError(t, err)

	file, _, err = c.Get(id)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)

	c, err = blobcache.NewCache(logger, cacheDir, sha256.New, sha256.Size, nil)
	require.NoError(t, err)

	err = c.Trim(1000)
	require.NoError(t, err)

	_, _, err = c.Get(id)
	require.Error(t, err)
}
