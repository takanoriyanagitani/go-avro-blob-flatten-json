package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-blob-flatten-json/util"

	bf "github.com/takanoriyanagitani/go-avro-blob-flatten-json/blob2json2flatmap"

	dh "github.com/takanoriyanagitani/go-avro-blob-flatten-json/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-blob-flatten-json/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var blobKey IO[string] = EnvValByKey("ENV_BLOB_KEY")

var stdin2avro2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var blob2flatmap bf.BlobToFlatMap = bf.BlobToFlatMapStdNewDefault()

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	blobKey,
	func(bkey string) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2avro2maps,
			blob2flatmap.BlobKeyToMapsToMaps(bkey),
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(
		func(filename string) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}
			var buf strings.Builder
			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		},
	)
}

const SchemaFileSizeLimitDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeLimitDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToWriterDefault(os.Stdout)(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
