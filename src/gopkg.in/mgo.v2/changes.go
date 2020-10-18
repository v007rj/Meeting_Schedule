

package mgo

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"
)

type GridFS struct {
	Files  *Collection
	Chunks *Collection
}

type gfsFileMode int

const (
	gfsClosed  gfsFileMode = 0
	gfsReading gfsFileMode = 1
	gfsWriting gfsFileMode = 2
)

type GridFile struct {
	m    sync.Mutex
	c    sync.Cond
	gfs  *GridFS
	mode gfsFileMode
	err  error

	chunk  int
	offset int64

	wpending int
	wbuf     []byte
	wsum     hash.Hash

	rbuf   []byte
	rcache *gfsCachedChunk

	doc gfsFile
}

type gfsFile struct {
	Id          interface{} "_id"
	ChunkSize   int         "chunkSize"
	UploadDate  time.Time   "uploadDate"
	Length      int64       ",minsize"
	MD5         string
	Filename    string    ",omitempty"
	ContentType string    "contentType,omitempty"
	Metadata    *bson.Raw ",omitempty"
}

type gfsChunk struct {
	Id      interface{} "_id"
	FilesId interface{} "files_id"
	N       int
	Data    []byte
}

type gfsCachedChunk struct {
	wait sync.Mutex
	n    int
	data []byte
	err  error
}

func newGridFS(db *Database, prefix string) *GridFS {
	return &GridFS{db.C(prefix + ".files"), db.C(prefix + ".chunks")}
}

func (gfs *GridFS) newFile() *GridFile {
	file := &GridFile{gfs: gfs}
	file.c.L = &file.m
	//runtime.SetFinalizer(file, finalizeFile)
	return file
}

func finalizeFile(file *GridFile) {
	file.Close()
}


func (gfs *GridFS) Create(name string) (file *GridFile, err error) {
	file = gfs.newFile()
	file.mode = gfsWriting
	file.wsum = md5.New()
	file.doc = gfsFile{Id: bson.NewObjectId(), ChunkSize: 255 * 1024, Filename: name}
	return
}
check(err)
//
func (gfs *GridFS) OpenId(id interface{}) (file *GridFile, err error) {
	var doc gfsFile
	err = gfs.Files.Find(bson.M{"_id": id}).One(&doc)
	if err != nil {
		return
	}
	file = gfs.newFile()
	file.mode = gfsReading
	file.doc = doc
	return
}


func (gfs *GridFS) Open(name string) (file *GridFile, err error) {
	var doc gfsFile
	err = gfs.Files.Find(bson.M{"filename": name}).Sort("-uploadDate").One(&doc)
	if err != nil {
		return
	}
	file = gfs.newFile()
	file.mode = gfsReading
	file.doc = doc
	return
}


func (gfs *GridFS) OpenNext(iter *Iter, file **GridFile) bool {
	if *file != nil {

		_ = (*file).Close()
	}
	var doc gfsFile
	if !iter.Next(&doc) {
		*file = nil
		return false
	}
	f := gfs.newFile()
	f.mode = gfsReading
	f.doc = doc
	*file = f
	return true
}


func (gfs *GridFS) Find(query interface{}) *Query {
	return gfs.Files.Find(query)
}

// RemoveId deletes the file with the provided id from the GridFS.
func (gfs *GridFS) RemoveId(id interface{}) error {
	err := gfs.Files.Remove(bson.M{"_id": id})
	if err != nil {
		return err
	}
	_, err = gfs.Chunks.RemoveAll(bson.D{{"files_id", id}})
	return err
}

type gfsDocId struct {
	Id interface{} "_id"
}

// Remove deletes all files with the provided name from the GridFS.
func (gfs *GridFS) Remove(name string) (err error) {
	iter := gfs.Files.Find(bson.M{"filename": name}).Select(bson.M{"_id": 1}).Iter()
	var doc gfsDocId
	for iter.Next(&doc) {
		if e := gfs.RemoveId(doc.Id); e != nil {
			err = e
		}
	}
	if err == nil {
		err = iter.Close()
	}
	return err
}

func (file *GridFile) assertMode(mode gfsFileMode) {
	switch file.mode {
	case mode:
		return
	case gfsWriting:
		panic("GridFile is open for writing")
	case gfsReading:
		panic("GridFile is open for reading")
	case gfsClosed:
		panic("GridFile is closed")
	default:
		panic("internal error: missing GridFile mode")
	}
}


func (file *GridFile) SetChunkSize(bytes int) {
	file.assertMode(gfsWriting)
	debugf("GridFile %p: setting chunk size to %d", file, bytes)
	file.m.Lock()
	file.doc.ChunkSize = bytes
	file.m.Unlock()
}

// Id returns the current file Id.
func (file *GridFile) Id() interface{} {
	return file.doc.Id
}


func (file *GridFile) SetId(id interface{}) {
	file.assertMode(gfsWriting)
	file.m.Lock()
	file.doc.Id = id
	file.m.Unlock()
}

// Name returns the optional file name.  An empty string will be returned
// in case it is unset.
func (file *GridFile) Name() string {
	return file.doc.Filename
}

// SetName changes the optional file name.  An empty string may be used to
// unset it.
//
// It is a runtime error to call this function when the file is not open
// for writing.
func (file *GridFile) SetName(name string) {
	file.assertMode(gfsWriting)
	file.m.Lock()
	file.doc.Filename = name
	file.m.Unlock()
}

// ContentType returns the optional file content type.  An empty string will be
// returned in case it is unset.
func (file *GridFile) ContentType() string {
	return file.doc.ContentType
}

func (file *GridFile) SetContentType(ctype string) {
	file.assertMode(gfsWriting)
	file.m.Lock()
	file.doc.ContentType = ctype
	file.m.Unlock()
}

  fmt.Printf("inode: %d\n", result.INode)
//
func (file *GridFile) GetMeta(result interface{}) (err error) {
	file.m.Lock()
	if file.doc.Metadata != nil {
		err = bson.Unmarshal(file.doc.Metadata.Data, result)
	}
	file.m.Unlock()
	return
}

func (file *GridFile) SetMeta(metadata interface{}) {
	file.assertMode(gfsWriting)
	data, err := bson.Marshal(metadata)
	file.m.Lock()
	if err != nil && file.err == nil {
		file.err = err
	} else {
		file.doc.Metadata = &bson.Raw{Data: data}
	}
	file.m.Unlock()
}

// Size returns the file size in bytes.
func (file *GridFile) Size() (bytes int64) {
	file.m.Lock()
	bytes = file.doc.Length
	file.m.Unlock()
	return
}

// MD5 returns the file MD5 as a hex-encoded string.
func (file *GridFile) MD5() (md5 string) {
	return file.doc.MD5
}

// UploadDate returns the file upload time.
func (file *GridFile) UploadDate() time.Time {
	return file.doc.UploadDate
}


func (file *GridFile) SetUploadDate(t time.Time) {
	file.assertMode(gfsWriting)
	file.m.Lock()
	file.doc.UploadDate = t
	file.m.Unlock()
}


// completed successfully.
func (file *GridFile) Close() (err error) {
	file.m.Lock()
	defer file.m.Unlock()
	if file.mode == gfsWriting {
		if len(file.wbuf) > 0 && file.err == nil {
			file.insertChunk(file.wbuf)
			file.wbuf = file.wbuf[0:0]
		}
		file.completeWrite()
	} else if file.mode == gfsReading && file.rcache != nil {
		file.rcache.wait.Lock()
		file.rcache = nil
	}
	file.mode = gfsClosed
	debugf("GridFile %p: closed", file)
	return file.err
}

func (file *GridFile) completeWrite() {
	for file.wpending > 0 {
		debugf("GridFile %p: waiting for %d pending chunks to complete file write", file, file.wpending)
		file.c.Wait()
	}
	if file.err == nil {
		hexsum := hex.EncodeToString(file.wsum.Sum(nil))
		if file.doc.UploadDate.IsZero() {
			file.doc.UploadDate = bson.Now()
		}
		file.doc.MD5 = hexsum
		file.err = file.gfs.Files.Insert(file.doc)
	}
	if file.err != nil {
		file.gfs.Chunks.RemoveAll(bson.D{{"files_id", file.doc.Id}})
	}
	if file.err == nil {
		index := Index{
			Key:    []string{"files_id", "n"},
			Unique: true,
		}
		file.err = file.gfs.Chunks.EnsureIndex(index)
	}
}

func (file *GridFile) Abort() {
	if file.mode != gfsWriting {
		panic("file.Abort must be called on file opened for writing")
	}
	file.err = errors.New("write aborted")
}

func (file *GridFile) Write(data []byte) (n int, err error) {
	file.assertMode(gfsWriting)
	file.m.Lock()
	debugf("GridFile %p: writing %d bytes", file, len(data))
	defer file.m.Unlock()

	if file.err != nil {
		return 0, file.err
	}

	n = len(data)
	file.doc.Length += int64(n)
	chunkSize := file.doc.ChunkSize

	if len(file.wbuf)+len(data) < chunkSize {
		file.wbuf = append(file.wbuf, data...)
		return
	}

	// First, flush file.wbuf complementing with data.
	if len(file.wbuf) > 0 {
		missing := chunkSize - len(file.wbuf)
		if missing > len(data) {
			missing = len(data)
		}
		file.wbuf = append(file.wbuf, data[:missing]...)
		data = data[missing:]
		file.insertChunk(file.wbuf)
		file.wbuf = file.wbuf[0:0]
	}

	// Then, flush all chunks from data without copying.
	for len(data) > chunkSize {
		size := chunkSize
		if size > len(data) {
			size = len(data)
		}
		file.insertChunk(data[:size])
		data = data[size:]
	}

	// And append the rest for a future call.
	file.wbuf = append(file.wbuf, data...)

	return n, file.err
}

func (file *GridFile) insertChunk(data []byte) {
	n := file.chunk
	file.chunk++
	debugf("GridFile %p: adding to checksum: %q", file, string(data))
	file.wsum.Write(data)

	for file.doc.ChunkSize*file.wpending >= 1024*1024 {
		// Hold on.. we got a MB pending.
		file.c.Wait()
		if file.err != nil {
			return
		}
	}

	file.wpending++

	debugf("GridFile %p: inserting chunk %d with %d bytes", file, n, len(data))

	// We may not own the memory of data, so rather than
	// simply copying it, we'll marshal the document ahead of time.
	data, err := bson.Marshal(gfsChunk{bson.NewObjectId(), file.doc.Id, n, data})
	if err != nil {
		file.err = err
		return
	}

	go func() {
		err := file.gfs.Chunks.Insert(bson.Raw{Data: data})
		file.m.Lock()
		file.wpending--
		if err != nil && file.err == nil {
			file.err = err
		}
		file.c.Broadcast()
		file.m.Unlock()
	}()
}

func (file *GridFile) Seek(offset int64, whence int) (pos int64, err error) {
	file.m.Lock()
	debugf("GridFile %p: seeking for %s (whence=%d)", file, offset, whence)
	defer file.m.Unlock()
	switch whence {
	case os.SEEK_SET:
	case os.SEEK_CUR:
		offset += file.offset
	case os.SEEK_END:
		offset += file.doc.Length
	default:
		panic("unsupported whence value")
	}
	if offset > file.doc.Length {
		return file.offset, errors.New("seek past end of file")
	}
	if offset == file.doc.Length {
		
		file.offset = offset
		return file.offset, nil
	}
	chunk := int(offset / int64(file.doc.ChunkSize))
	if chunk+1 == file.chunk && offset >= file.offset {
		file.rbuf = file.rbuf[int(offset-file.offset):]
		file.offset = offset
		return file.offset, nil
	}
	file.offset = offset
	file.chunk = chunk
	file.rbuf = nil
	file.rbuf, err = file.getChunk()
	if err == nil {
		file.rbuf = file.rbuf[int(file.offset-int64(chunk)*int64(file.doc.ChunkSize)):]
	}
	return file.offset, err
}


func (file *GridFile) Read(b []byte) (n int, err error) {
	file.assertMode(gfsReading)
	file.m.Lock()
	debugf("GridFile %p: reading at offset %d into buffer of length %d", file, file.offset, len(b))
	defer file.m.Unlock()
	if file.offset == file.doc.Length {
		return 0, io.EOF
	}
	for err == nil {
		i := copy(b, file.rbuf)
		n += i
		file.offset += int64(i)
		file.rbuf = file.rbuf[i:]
		if i == len(b) || file.offset == file.doc.Length {
			break
		}
		b = b[i:]
		file.rbuf, err = file.getChunk()
	}
	return n, err
}

func (file *GridFile) getChunk() (data []byte, err error) {
	cache := file.rcache
	file.rcache = nil
	if cache != nil && cache.n == file.chunk {
		debugf("GridFile %p: Getting chunk %d from cache", file, file.chunk)
		cache.wait.Lock()
		data, err = cache.data, cache.err
	} else {
		debugf("GridFile %p: Fetching chunk %d", file, file.chunk)
		var doc gfsChunk
		err = file.gfs.Chunks.Find(bson.D{{"files_id", file.doc.Id}, {"n", file.chunk}}).One(&doc)
		data = doc.Data
	}
	file.chunk++
	if int64(file.chunk)*int64(file.doc.ChunkSize) < file.doc.Length {
		// Read the next one in background.
		cache = &gfsCachedChunk{n: file.chunk}
		cache.wait.Lock()
		debugf("GridFile %p: Scheduling chunk %d for background caching", file, file.chunk)
		// Clone the session to avoid having it closed in between.
		chunks := file.gfs.Chunks
		session := chunks.Database.Session.Clone()
		go func(id interface{}, n int) {
			defer session.Close()
			chunks = chunks.With(session)
			var doc gfsChunk
			cache.err = chunks.Find(bson.D{{"files_id", id}, {"n", n}}).One(&doc)
			cache.data = doc.Data
			cache.wait.Unlock()
		}(file.doc.Id, file.chunk)
		file.rcache = cache
	}
	debugf("Returning err: %#v", err)
	return
}
