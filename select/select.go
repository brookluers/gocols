// Select creates a copy of a columnized dataset, retaining only those
// records where the value of the index variable belongs to a given
// set.

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/golang/snappy"
	"github.com/kshedden/gocols/config"
)

const (
	concurrency = 20
)

var (
	// The name of the variable whose values will determine which
	// records are selected.
	idvar string

	// The specific values of the selection variable to retain.
	ids []uint64

	// File name containing ids
	idfile string

	// The directory where the selected data will be stored
	targetdir string

	// The directory where the full data are stored
	sourcedir string

	// Configuration information for the source dat
	conf *config.Config

	// If true, overwrite existing files
	replace bool

	// Logging
	logger *log.Logger

	sem chan bool
)

func setupLogger() {
	fn := "select.log"
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Ltime)
}

type Sl64 []uint64

func (a Sl64) Len() int           { return len(a) }
func (a Sl64) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Sl64) Less(i, j int) bool { return a[i] < a[j] }

// getids reads the id values that will be included in the target data
// set.
func getids(idfile string) {

	fid, err := os.Open(idfile)
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	scanner := bufio.NewScanner(fid)

	for scanner.Scan() {
		id, err := strconv.Atoi(scanner.Text())
		if err != nil {
			panic(err)
		}
		ids = append(ids, uint64(id))
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	sort.Sort(Sl64(ids))
}

// setupTargetDir creates the directory layout where the selected
// cases will be written.
func setupTargetDir() {

	p := path.Join(targetdir, "Buckets")
	err := os.MkdirAll(p, 0755)
	if err != nil {
		panic(err)
	}

	for k := 0; k < conf.NumBuckets; k++ {
		q := config.BucketPath(k, targetdir)
		err = os.MkdirAll(q, 0755)
		if err != nil {
			panic(err)
		}
	}
}

// contains returns true if and only if v is an element of a, where a
// is a sorted array.
func contains(a []uint64, v uint64) bool {

	f := func(i int) bool {
		return a[i] >= v
	}

	k := sort.Search(len(a), f)
	if k >= len(a) {
		return false
	}
	return a[k] == v
}

// getix returns a boolean vector indicating which values should be selected
func getix(bn int) []bool {

	fn := config.BucketPath(bn, sourcedir)
	fn = path.Join(fn, fmt.Sprintf("%s.bin.sz", idvar))
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	rdr := snappy.NewReader(fid)

	var ix []bool
	var m, n int
	for {
		var x uint64
		err := binary.Read(rdr, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		f := contains(ids, x)
		ix = append(ix, f)
		if f {
			m++
		}
		n++
	}

	logger.Printf("Selected %d out of %d rows from bucket %d\n", m, n, bn)

	return ix
}

// dofixedwith selects the values of interest from the given variable
// in the source directory, and writes only those values to the target
// directory.  This function operates on any slice of fixed width
// values.
func dofixedwidth(bn int, vname string, w int, ix []bool) {

	// Input
	rdr, fid1 := getreader(bn, vname)
	defer fid1.Close()

	// Output
	wtr, fid2 := getwriter(bn, vname)
	defer fid2.Close()
	defer wtr.Close()

	b := make([]byte, w)

	for _, ii := range ix {
		_, err := io.ReadFull(rdr, b)
		if err != nil {
			panic(err)
		}

		if !ii {
			continue
		}

		err = binary.Write(wtr, binary.LittleEndian, b)
		if err != nil {
			panic(err)
		}
	}
}

// getreader returns a reader, closer pair for the source directory.
func getreader(bn int, vname string) (io.Reader, io.Closer) {
	fn := config.BucketPath(bn, sourcedir)
	fn = path.Join(fn, fmt.Sprintf("%s.bin.sz", vname))
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	rdr := snappy.NewReader(fid)
	return rdr, fid
}

// getwriter returns a writer, closer pair for the target directory.
func getwriter(bn int, vname string) (io.WriteCloser, io.Closer) {
	fn := config.BucketPath(bn, targetdir)
	fn = path.Join(fn, fmt.Sprintf("%s.bin.sz", vname))
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	wtr := snappy.NewBufferedWriter(fid)
	return wtr, fid
}

// douvarint selects the values of interest for a variable of type
// uvarint from the source directory, and writes them to the target
// directory.
func douvarint(bn int, vname string, ix []bool) {

	// Input
	rdr, fid1 := getreader(bn, vname)
	defer fid1.Close()
	br := bufio.NewReader(rdr)

	// Output
	wtr, fid2 := getwriter(bn, vname)
	defer fid2.Close()
	defer wtr.Close()

	b := make([]byte, 8)

	for _, ii := range ix {
		x, err := binary.ReadUvarint(br)
		if err != nil {
			panic(err)
		}

		if !ii {
			continue
		}

		m := binary.PutUvarint(b, x)
		_, err = wtr.Write(b[0:m])
		if err != nil {
			panic(err)
		}
	}
}

func writedtypes(dtypes map[string]string, bn int) {

	fn := config.BucketPath(bn, targetdir)
	fn = path.Join(fn, "dtypes.json")
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	enc := json.NewEncoder(fid)
	err = enc.Encode(dtypes)
	if err != nil {
		panic(err)
	}
}

// dobucket does the selection on one bucket
func dobucket(bn int) {

	defer func() { <-sem }()

	dtypes := config.ReadDtypes(bn, sourcedir)

	writedtypes(dtypes, bn)

	ix := getix(bn)

	for vn, dt := range dtypes {

		if dt == "uvarint" {
			douvarint(bn, vn, ix)
		} else if dt == "varint" {
			panic("varint not implemented\n")
		} else {
			w := config.DTsize[dt]
			dofixedwidth(bn, vn, w, ix)
		}
	}
}

// copycodes makes a copy in the target directory of all the files in
// the Codes directory of the source data (labels for factor-coded
// variables and related meta-data).
func copycodes() {

	sp := conf.CodesDir

	dp := path.Join(targetdir, "Codes")
	os.MkdirAll(dp, 0755)

	fl, err := ioutil.ReadDir(sp)
	if err != nil {
		panic(err)
	}

	for _, fi := range fl {

		fn := fi.Name()

		fid, err := os.Open(path.Join(sp, fn))
		if err != nil {
			panic(err)
		}
		defer fid.Close()

		gid, err := os.Create(path.Join(dp, fn))
		if err != nil {
			panic(err)
		}
		defer gid.Close()

		_, err = io.Copy(gid, fid)
		if err != nil {
			panic(err)
		}
	}
}

func check() {

	if !replace {
		_, err := os.Stat(targetdir)
		if !os.IsNotExist(err) {
			fmt.Printf("Use -replace=true to overwrite existing contents of %s\n\n", targetdir)
			os.Exit(1)
		}
	}

	ts, err := os.Stat(targetdir)
	if err != nil {
		ss, err := os.Stat(sourcedir)
		if err != nil {
			if os.SameFile(ts, ss) {
				os.Stderr.WriteString("Cannot have targetdir equal to sourcedir\n")
				os.Exit(1)
			}
		}
	}
}

func main() {

	flag.StringVar(&idvar, "idvar", "", "variable to select on")
	flag.StringVar(&idfile, "idfile", "", "file path to values to select")
	flag.StringVar(&targetdir, "targetdir", "", "destination directory")
	flag.StringVar(&sourcedir, "sourcedir", "", "source directory")
	flag.BoolVar(&replace, "replace", false, "overwrite existing files")
	flag.Parse()

	if idvar == "" || idfile == "" || targetdir == "" || sourcedir == "" {
		msg := fmt.Sprintf("usage:\nselect idvar idfile targetdir sourcedir\n\n")
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}

	check()

	setupLogger()
	os.MkdirAll(targetdir, 0755)

	conf = config.GetConfig(sourcedir)

	// Modify the conf for the target directory and save it there.
	var tconf config.Config
	tconf = *conf
	tconf.CodesDir = path.Join(targetdir, "Codes")
	config.WriteConfig(targetdir, &tconf)

	sem = make(chan bool, concurrency)

	copycodes()
	getids(idfile)

	setupTargetDir()

	for k := 0; k < conf.NumBuckets; k++ {
		sem <- true
		go dobucket(k)
	}

	for k := 0; k < concurrency; k++ {
		sem <- true
	}

	logger.Printf("Done, exiting")
}
