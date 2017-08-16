package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type Config struct {

	// The number of buckets in the data set
	NumBuckets int

	// The compression type for the raw data
	Compression string

	// The path where corresponding factor code information is
	// stored
	CodesDir string
}

var (
	// Size in bytes of each data type.
	DTsize = map[string]int{"uint8": 1, "uint16": 2, "uint32": 4, "uint64": 8, "float32": 4, "float64": 8}
)

// GetConfig reads a configuration file from the given path and returns it.
func GetConfig(pa string) *Config {

	fid, err := os.Open(path.Join(pa, "conf.json"))
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	dec := json.NewDecoder(fid)
	conf := new(Config)
	err = dec.Decode(conf)
	if err != nil {
		panic(err)
	}
	return conf
}

// WriteConfig writes the given configuration file to the provided path.
func WriteConfig(pa string, conf *Config) {

	fid, err := os.Create(path.Join(pa, "conf.json"))
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	enc := json.NewEncoder(fid)
	err = enc.Encode(conf)
	if err != nil {
		panic(err)
	}
}

// BucketPath returns the path to the given bucket.
func BucketPath(bucket int, pa string) string {
	b := fmt.Sprintf("%04d", bucket)
	return path.Join(pa, "Buckets", b)
}

// ReadDtypes returns a map describing the column data types map for a
// given bucket.  The dtypes map associates variable names with their
// data type (e.g. uint8).
func ReadDtypes(bucket int, pa string) map[string]string {

	dtypes := make(map[string]string)

	p := BucketPath(bucket, pa)
	fn := path.Join(p, "dtypes.json")

	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	dec := json.NewDecoder(fid)
	err = dec.Decode(&dtypes)
	if err != nil {
		panic(err)
	}

	return dtypes
}

// GetFactorCodes returns a map from strings to integers describing a
// factor-coded variable.
func GetFactorCodes(varname string, conf *Config) map[string]int {

	// Determine the code group
	pa := path.Join(conf.CodesDir, "CodeFiles.json")
	fid, err := os.Open(pa)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	dec := json.NewDecoder(fid)
	cf := make(map[string]string)
	err = dec.Decode(&cf)
	if err != nil {
		panic(err)
	}
	grp, ok := cf[varname]
	if !ok {
		// If varname is not a variable name, assume it is a group name
		grp = varname
	}

	// Read the codes
	pa = path.Join(conf.CodesDir, grp+"Codes.json")
	fid, err = os.Open(pa)
	if err != nil {
		msg := fmt.Sprintf("Can't open codes file %s\n", pa)
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}
	defer fid.Close()

	dec = json.NewDecoder(fid)
	mp := make(map[string]int)
	err = dec.Decode(&mp)
	if err != nil {
		panic(err)
	}

	return mp
}

// RevFactorCodes returns the reverse factor coding map, associating
// integers with their corresponding string label.
func RevCodes(codes map[string]int) map[int]string {

	rcodes := make(map[int]string)

	for k, v := range codes {
		rcodes[v] = k
	}

	return rcodes
}
