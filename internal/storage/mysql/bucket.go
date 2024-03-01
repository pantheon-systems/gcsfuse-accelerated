// Copyright 2023 Google Inc. All Rights Reserved.
// Copyright 2024 Pantheon Systems Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
	"github.com/jacobsa/timeutil"

	"database/sql"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Equivalent to NewConn(clock).GetBucket(name).
func NewMysqlBucket(clock timeutil.Clock, name string) gcs.Bucket {
	// @TODO: Move these to function parameters and harvest the environment variables outside this code.
	dbUser := os.Getenv("DATABASE_USER")
	dbPassword := os.Getenv("DATABASE_PASSWORD")
	dbHost := "localhost"
	dbPort := "3306"
	dbName := os.Getenv("DATABASE_DB")

	db, err := sql.Open("mysql", dbUser+":"+dbPassword+"@tcp("+dbHost+":"+dbPort+")/"+dbName)
	if err != nil {
		panic(err)
	}

	// Create the table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255), value BLOB, PRIMARY KEY (key))", name))
	if err != nil {
		panic(err)
	}

	b := &bucket{
		clock: clock,
		name:  name,
		db:    db,
	}
	return b
}

////////////////////////////////////////////////////////////////////////
// Helper types
////////////////////////////////////////////////////////////////////////

type mysqlObject struct {
	metadata gcs.Object
	data     []byte
}

// Return the smallest string that is lexicographically larger than prefix and
// does not have prefix as a prefix. For the sole case where this is not
// possible (all strings consisting solely of 0xff bytes, including the empty
// string), return the empty string.
func prefixSuccessor(prefix string) string {
	// Attempt to increment the last byte. If that is a 0xff byte, erase it and
	// recurse. If we hit an empty string, then we know our task is impossible.
	limit := []byte(prefix)
	for len(limit) > 0 {
		b := limit[len(limit)-1]
		if b != 0xff {
			limit[len(limit)-1]++
			break
		}

		limit = limit[:len(limit)-1]
	}

	return string(limit)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

type bucket struct {
	clock timeutil.Clock
	name  string

	// The database of extant objects.
	db *sql.DB

	// The most recent generation number that was minted. The next object will
	// receive generation prevGeneration + 1.
	//
	// INVARIANT: This is an upper bound for generation numbers in objects.

	// @TODO: Replace with counter in the database.
	prevGeneration int64 // GUARDED_BY(mu)

}

func (b *bucket) getObject(name string, forUpdate bool, tx *sql.Tx) (obj *mysqlObject, err error) {
	forUpdateClause := ""
	if forUpdate {
		forUpdateClause = "FOR UPDATE"
	}

	// Find the object with the requested name.
	rows, err := tx.Query("SELECT "+forUpdateClause+" value FROM "+b.name+" WHERE key = ?", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		obj, err = rowToRecord(rows)
		if err != nil {
			return nil, err
		}
	}

	return
}

func rowToRecord(row *sql.Rows) (obj *mysqlObject, err error) {
	var serializedData bytes.Buffer
	obj = nil
	if err := row.Scan(&serializedData); err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(&serializedData)
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func checkName(name string) (err error) {
	if len(name) == 0 || len(name) > 1024 {
		err = errors.New("Invalid object name: length must be in [1, 1024]")
		return
	}

	if !utf8.ValidString(name) {
		err = errors.New("Invalid object name: not valid UTF-8")
		return
	}

	for _, r := range name {
		if r == 0x0a || r == 0x0d {
			err = errors.New("Invalid object name: must not contain CR or LF")
			return
		}
	}

	return
}

// Create an object struct for the given attributes and contents.
// Wrapping transaction required.
func (b *bucket) mintObject(
	req *gcs.CreateObjectRequest,
	contents []byte) (o mysqlObject) {
	md5Sum := md5.Sum(contents)
	crc32c := crc32.Checksum(contents, crc32cTable)

	// Set up basic info.
	b.prevGeneration++
	o.metadata = gcs.Object{
		Name:            req.Name,
		ContentType:     req.ContentType,
		ContentLanguage: req.ContentLanguage,
		CacheControl:    req.CacheControl,
		Owner:           "user-mysql",
		Size:            uint64(len(contents)),
		ContentEncoding: req.ContentEncoding,
		ComponentCount:  1,
		MD5:             &md5Sum,
		CRC32C:          &crc32c,
		MediaLink:       "http://localhost/download/storage/mysql/" + req.Name,
		Metadata:        copyMetadata(req.Metadata),
		Generation:      b.prevGeneration,
		MetaGeneration:  1,
		StorageClass:    "STANDARD",
		Updated:         b.clock.Now(),
	}

	// Set up data.
	o.data = contents

	return
}

func (b *bucket) createObjectLocked(
	req *gcs.CreateObjectRequest, tx *sql.Tx) (o *gcs.Object, err error) {
	// Check that the name is legal.
	err = checkName(req.Name)
	if err != nil {
		return
	}

	// Snarf the contents.
	contents, err := io.ReadAll(req.Contents)
	if err != nil {
		err = fmt.Errorf("ReadAll: %v", err)
		return
	}

	// Find any existing record for this name.
	rows, err := tx.Query("SELECT FOR UPDATE value FROM "+b.name+" WHERE key = ?", req.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var existingRecord *mysqlObject
	for rows.Next() {
		existingRecord, err = rowToRecord(rows)
		if err != nil {
			return nil, err
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Check the provided checksum, if any.
	if req.CRC32C != nil {
		actual := crc32.Checksum(contents, crc32cTable)
		if actual != *req.CRC32C {
			err = fmt.Errorf(
				"CRC32C mismatch: got 0x%08x, expected 0x%08x",
				actual,
				*req.CRC32C)

			return
		}
	}

	// Check the provided hash, if any.
	if req.MD5 != nil {
		actual := md5.Sum(contents)
		if actual != *req.MD5 {
			err = fmt.Errorf(
				"MD5 mismatch: got %s, expected %s",
				hex.EncodeToString(actual[:]),
				hex.EncodeToString(req.MD5[:]))

			return
		}
	}

	// Check preconditions.
	if req.GenerationPrecondition != nil {
		if *req.GenerationPrecondition == 0 && existingRecord != nil {
			err = &gcs.PreconditionError{
				Err: errors.New("Precondition failed: object exists"),
			}

			return
		}

		if *req.GenerationPrecondition > 0 {
			if existingRecord == nil {
				err = &gcs.PreconditionError{
					Err: errors.New("Precondition failed: object doesn't exist"),
				}

				return
			}

			existingGen := existingRecord.metadata.Generation
			if existingGen != *req.GenerationPrecondition {
				err = &gcs.PreconditionError{
					Err: fmt.Errorf(
						"Precondition failed: object has generation %v",
						existingGen),
				}

				return
			}
		}
	}

	if req.MetaGenerationPrecondition != nil {
		if existingRecord == nil {
			err = &gcs.PreconditionError{
				Err: errors.New("Precondition failed: object doesn't exist"),
			}

			return
		}

		existingMetaGen := existingRecord.metadata.MetaGeneration
		if existingMetaGen != *req.MetaGenerationPrecondition {
			err = &gcs.PreconditionError{
				Err: fmt.Errorf(
					"Precondition failed: object has meta-generation %v",
					existingMetaGen),
			}

			return
		}
	}

	// Create an object record from the given attributes.
	var fo mysqlObject = b.mintObject(req, contents)
	o = copyObject(&fo.metadata)

	// Serialize the data.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(fo); err != nil {
		return nil, err
	}

	// Replace an entry in or add an entry to our database of objects.
	_, err = tx.Query("INSERT INTO "+b.name+" (key, value) VALUES (%s, %b) ON DUPLICATE KEY UPDATE value = %b", req.Name, buf, buf)
	if err != nil {
		return nil, err
	}

	return
}

// Create a reader based on the supplied request, also returning the index
// within b.objects of the entry for the requested generation.
//
// LOCKS_REQUIRED(b.mu)
func (b *bucket) newReaderLocked(
	req *gcs.ReadObjectRequest, tx *sql.Tx) (r io.Reader, existingRecord *mysqlObject, err error) {

	// Find the object with the requested name.
	rows, err := tx.Query("SELECT FOR UPDATE value FROM "+b.name+" WHERE key = ?", req.Name)
	if err != nil {
		return nil, nil, err
	}

	for rows.Next() {
		existingRecord, err = rowToRecord(rows)
		if err != nil {
			return nil, nil, err
		}
	}

	if existingRecord == nil {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}

	o := existingRecord

	// Does the generation match?
	if req.Generation != 0 && req.Generation != o.metadata.Generation {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf(
				"Object %s generation %v not found", req.Name, req.Generation),
		}

		return
	}

	// Extract the requested range.
	result := o.data

	if req.Range != nil {
		start := req.Range.Start
		limit := req.Range.Limit
		l := uint64(len(result))

		if start > limit {
			start = 0
			limit = 0
		}

		if start > l {
			start = 0
			limit = 0
		}

		if limit > l {
			limit = l
		}

		result = result[start:limit]
	}

	r = bytes.NewReader(result)

	return
}

func copyMetadata(in map[string]string) (out map[string]string) {
	if in == nil {
		return
	}

	out = make(map[string]string)
	for k, v := range in {
		out[k] = v
	}

	return
}

func copyObject(o *gcs.Object) *gcs.Object {
	var copy gcs.Object = *o
	copy.Metadata = copyMetadata(o.Metadata)
	return &copy
}

////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////

func (b *bucket) Name() string {
	return b.name
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) ListObjects(
	ctx context.Context,
	req *gcs.ListObjectsRequest) (listing *gcs.Listing, err error) {

	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Set up the result object.
	listing = new(gcs.Listing)

	// Handle defaults.
	maxResults := req.MaxResults
	if maxResults == 0 {
		maxResults = 1000
	}

	// Find where in the space of object names to start.
	nameStart := req.Prefix
	if req.ContinuationToken != "" && req.ContinuationToken > nameStart {
		nameStart = req.ContinuationToken
	}

	// Find the object with the requested name.
	rows, err := tx.Query("SELECT value FROM "+b.name+" WHERE key LIKE ?% LIMIT %d", nameStart, maxResults+1)
	if err != nil {
		return nil, err
	}

	// Find the range of indexes within the array to scan.
	//prefixLimit := b.objects.prefixUpperBound(req.Prefix)
	//indexLimit := minInt(indexStart+maxResults, prefixLimit)

	// Scan the array.
	var lastResultWasPrefix bool
	remainingResults := maxResults
	for rows.Next() {
		o, err := rowToRecord(rows)
		if err != nil {
			return nil, err
		}

		name := o.metadata.Name

		// Search for a delimiter if necessary.
		if req.Delimiter != "" {
			// Search only in the part after the prefix.
			nameMinusQueryPrefix := name[len(req.Prefix):]

			delimiterIndex := strings.Index(nameMinusQueryPrefix, req.Delimiter)
			if delimiterIndex >= 0 {
				resultPrefixLimit := delimiterIndex

				// Transform to an index within name.
				resultPrefixLimit += len(req.Prefix)

				// Include the delimiter in the result.
				resultPrefixLimit += len(req.Delimiter)

				// Save the result, but only if it's not a duplicate.
				resultPrefix := name[:resultPrefixLimit]
				if len(listing.CollapsedRuns) == 0 ||
					listing.CollapsedRuns[len(listing.CollapsedRuns)-1] != resultPrefix {
					listing.CollapsedRuns = append(listing.CollapsedRuns, resultPrefix)
				}

				isTrailingDelimiter := (delimiterIndex == len(nameMinusQueryPrefix)-1)
				if !isTrailingDelimiter || !req.IncludeTrailingDelimiter {
					lastResultWasPrefix = true
					continue
				}
			}
		}

		lastResultWasPrefix = false

		// Otherwise, return as an object result. Make a copy to avoid handing back
		// internal state.
		listing.Objects = append(listing.Objects, copyObject(&o.metadata))
		remainingResults--

		if remainingResults == 0 {
			break
		}
	}

	// Set up a cursor for where to start the next scan if we didn't exhaust the
	// results.
	if rows.Next() {
		// If the final object we visited was returned as an element in
		// listing.CollapsedRuns, we want to skip all other objects that would
		// result in the same so we don't return duplicate elements in
		// listing.CollapsedRuns across requests.
		if lastResultWasPrefix {
			lastResultPrefix := listing.CollapsedRuns[len(listing.CollapsedRuns)-1]
			listing.ContinuationToken = prefixSuccessor(lastResultPrefix)

			// Check an assumption: prefixSuccessor cannot result in the empty string
			// above because object names must be non-empty UTF-8 strings, and there
			// is no valid non-empty UTF-8 string that consists of entirely 0xff
			// bytes.
			if listing.ContinuationToken == "" {
				err = errors.New("Unexpected empty string from prefixSuccessor")
				return
			}
		} else {
			// Otherwise, we'll start scanning at the next object.
			o, err := rowToRecord(rows)
			if err != nil {
				return nil, err
			}
			listing.ContinuationToken = o.metadata.Name
		}
	}

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) NewReader(
	ctx context.Context,
	req *gcs.ReadObjectRequest) (rc io.ReadCloser, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	r, _, err := b.newReaderLocked(req, tx)
	if err != nil {
		return
	}

	rc = io.NopCloser(r)
	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) CreateObject(
	ctx context.Context,
	req *gcs.CreateObjectRequest) (o *gcs.Object, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	o, err = b.createObjectLocked(req, tx)
	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) CopyObject(
	ctx context.Context,
	req *gcs.CopyObjectRequest) (o *gcs.Object, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Check that the destination name is legal.
	err = checkName(req.DstName)
	if err != nil {
		return
	}

	// Does the object exist?
	rows, err := tx.Query("SELECT FOR UPDATE value FROM "+b.name+" WHERE key = ?", req.SrcName)
	if err != nil {
		return nil, err
	}

	var src *mysqlObject
	for rows.Next() {
		src, err = rowToRecord(rows)
		if err != nil {
			return nil, err
		}
	}

	if src == nil {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %q not found", req.SrcName),
		}

		return
	}

	// Does it have the correct generation?
	if req.SrcGeneration != 0 &&
		src.metadata.Generation != req.SrcGeneration {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf(
				"Object %s generation %d not found", req.SrcName, req.SrcGeneration),
		}

		return
	}

	// Does it have the correct meta-generation?
	if req.SrcMetaGenerationPrecondition != nil {
		p := *req.SrcMetaGenerationPrecondition
		if src.metadata.MetaGeneration != p {
			err = &gcs.PreconditionError{
				Err: fmt.Errorf(
					"Object %q has meta-generation %d",
					req.SrcName,
					src.metadata.MetaGeneration),
			}

			return
		}
	}

	// Copy it and assign a new generation number, to ensure that the generation
	// number for the destination name is strictly increasing.
	dst := src
	dst.metadata.Name = req.DstName
	dst.metadata.MediaLink = "http://localhost/download/storage/mysql/" + req.DstName

	b.prevGeneration++
	dst.metadata.Generation = b.prevGeneration

	// Serialize the data.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(dst); err != nil {
		return nil, err
	}

	// Replace an entry in or add an entry to our database of objects.
	_, err = tx.Query("INSERT INTO "+b.name+" (key, value) VALUES (%s, %b) ON DUPLICATE KEY UPDATE value = %b", req.DstName, buf, buf)
	if err != nil {
		return nil, err
	}

	o = copyObject(&dst.metadata)
	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) ComposeObjects(
	ctx context.Context,
	req *gcs.ComposeObjectsRequest) (o *gcs.Object, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// GCS doesn't like too few or too many sources.
	if len(req.Sources) < 1 {
		err = errors.New("You must provide at least one source component")
		return
	}

	if len(req.Sources) > gcs.MaxSourcesPerComposeRequest {
		err = errors.New("You have provided too many source components")
		return
	}

	// Find readers for all of the source objects, also computing the sum of
	// their component counts.
	var srcReaders []io.Reader
	var dstComponentCount int64

	for _, src := range req.Sources {
		var r io.Reader
		var srcObj *mysqlObject

		r, srcObj, err = b.newReaderLocked(&gcs.ReadObjectRequest{
			Name:       src.Name,
			Generation: src.Generation,
		}, tx)

		if err != nil {
			return
		}

		srcReaders = append(srcReaders, r)
		dstComponentCount += srcObj.metadata.ComponentCount
	}

	// GCS doesn't like the component count to go too high.
	if dstComponentCount > gcs.MaxComponentCount {
		err = errors.New("Result would have too many components")
		return
	}

	// Create the new object.
	createReq := &gcs.CreateObjectRequest{
		Name:                       req.DstName,
		GenerationPrecondition:     req.DstGenerationPrecondition,
		MetaGenerationPrecondition: req.DstMetaGenerationPrecondition,
		Contents:                   io.MultiReader(srcReaders...),
		ContentType:                req.ContentType,
		Metadata:                   req.Metadata,
	}

	_, err = b.createObjectLocked(createReq, tx)
	if err != nil {
		return
	}

	dstObj, err := b.getObject(req.DstName, false, tx)
	if err != nil {
		return
	}

	metadata := dstObj.metadata

	// Touchup: fix the component count.
	metadata.ComponentCount = dstComponentCount

	// Touchup: emulate the real GCS behavior of not exporting an MD5 hash for
	// composite objects.
	metadata.MD5 = nil

	o = copyObject(&metadata)
	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) StatObject(
	ctx context.Context,
	req *gcs.StatObjectRequest) (o *gcs.Object, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Does the object exist?
	obj, err := b.getObject(req.Name, false, tx)
	if err != nil {
		return
	}

	if obj != nil {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}

	// Make a copy to avoid handing back internal state.
	o = copyObject(&obj.metadata)

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) UpdateObject(
	ctx context.Context,
	req *gcs.UpdateObjectRequest) (o *gcs.Object, err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Does the object exist?
	existingObj, err := b.getObject(req.Name, true, tx)
	if err != nil {
		return
	}

	if existingObj != nil {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf("Object %s not found", req.Name),
		}

		return
	}
	var obj *gcs.Object = &existingObj.metadata

	// Does the generation number match the request?
	if req.Generation != 0 && obj.Generation != req.Generation {
		err = &gcs.NotFoundError{
			Err: fmt.Errorf(
				"Object %q generation %d not found",
				req.Name,
				req.Generation),
		}

		return
	}

	// Does the meta-generation precondition check out?
	if req.MetaGenerationPrecondition != nil &&
		obj.MetaGeneration != *req.MetaGenerationPrecondition {
		err = &gcs.PreconditionError{
			Err: fmt.Errorf(
				"Object %q has meta-generation %d",
				obj.Name,
				obj.MetaGeneration),
		}

		return
	}

	// Update the entry's basic fields according to the request.
	if req.ContentType != nil {
		obj.ContentType = *req.ContentType
	}

	if req.ContentEncoding != nil {
		obj.ContentEncoding = *req.ContentEncoding
	}

	if req.ContentLanguage != nil {
		obj.ContentLanguage = *req.ContentLanguage
	}

	if req.CacheControl != nil {
		obj.CacheControl = *req.CacheControl
	}

	// Update the user metadata if necessary.
	if len(req.Metadata) > 0 {
		if obj.Metadata == nil {
			obj.Metadata = make(map[string]string)
		}

		for k, v := range req.Metadata {
			if v == nil {
				delete(obj.Metadata, k)
				continue
			}

			obj.Metadata[k] = *v
		}
	}

	// Bump up the entry generation number and the update time.
	obj.MetaGeneration++
	obj.Updated = b.clock.Now()

	// Serialize the data.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(obj); err != nil {
		return
	}

	// Replace the existing entry.
	_, err = tx.Query("UPDATE "+b.name+" SET value = %b", req.Name, buf, buf)
	if err != nil {
		return
	}

	// Make a copy to avoid handing back internal state.
	o = copyObject(obj)

	return
}

// LOCKS_EXCLUDED(b.mu)
func (b *bucket) DeleteObject(
	ctx context.Context,
	req *gcs.DeleteObjectRequest) (err error) {
	tx, err := b.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// Does the object exist?
	obj, err := b.getObject(req.Name, true, tx)
	if err != nil {
		return
	}
	if obj == nil {
		return
	}

	// Don't do anything if the generation is wrong.
	if req.Generation != 0 &&
		obj.metadata.Generation != req.Generation {
		return
	}

	// Check the meta-generation if requested.
	if req.MetaGenerationPrecondition != nil {
		p := *req.MetaGenerationPrecondition
		if obj.metadata.MetaGeneration != p {
			err = &gcs.PreconditionError{
				Err: fmt.Errorf(
					"Object %q has meta-generation %d",
					req.Name,
					obj.metadata.MetaGeneration),
			}

			return
		}
	}

	// Remove the object.
	_, err = tx.Query("DELETE FROM "+b.name+" WHERE key = %s", req.Name)
	if err != nil {
		return
	}

	return
}
