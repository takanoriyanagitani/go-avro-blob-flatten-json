package blob2fmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"

	fj "github.com/takanoriyanagitani/go-flatten-json"

	bj "github.com/takanoriyanagitani/go-avro-blob-flatten-json"
	. "github.com/takanoriyanagitani/go-avro-blob-flatten-json/util"
)

var (
	ErrInvalidBlob error = errors.New("invalid blob")
)

type FlattenMap fj.MapToFlatAny

type BlobToFlatMap func(bj.OriginalBlob) IO[bj.FlatMap]

type BlobToOriginalMap func(bj.OriginalBlob) IO[bj.OriginalMap]

func (b BlobToOriginalMap) ToBlobToFlatMap(f FlattenMap) BlobToFlatMap {
	return func(o bj.OriginalBlob) IO[bj.FlatMap] {
		return func(ctx context.Context) (bj.FlatMap, error) {
			return Bind(
				b(o),
				Lift(func(omap bj.OriginalMap) (bj.FlatMap, error) {
					return f(omap), nil
				}),
			)(ctx)
		}
	}
}

func BlobToOriginalMapStdNew() BlobToOriginalMap {
	buf := map[string]any{}
	return Lift(func(ob bj.OriginalBlob) (bj.OriginalMap, error) {
		clear(buf)

		e := json.Unmarshal(ob, &buf)
		return buf, e
	})
}

func BlobToFlatMapStdNewDefault() BlobToFlatMap {
	var b2om BlobToOriginalMap = BlobToOriginalMapStdNew()

	var m2fm fj.MapToFlatMap = fj.MapToFlatMapDefault()
	var m2fa fj.MapToFlatAny = m2fm.ToMapToFlatAny()

	return b2om.ToBlobToFlatMap(FlattenMap(m2fa))
}

var EmptyMap map[string]any = map[string]any{}

func (f BlobToFlatMap) MapsToMaps(
	m iter.Seq2[map[string]any, error],
	jsonMapBlobKey string,
) IO[iter.Seq2[map[string]any, error]] {
	var bbuf bytes.Buffer
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}

			for row, e := range m {
				clear(buf)

				if nil != e {
					yield(buf, e)
					return
				}

				for key, val := range row {
					if jsonMapBlobKey != key {
						buf[key] = val
					}
				}

				bbuf.Reset()
				var jsonMapBlob any = row[jsonMapBlobKey]
				switch t := jsonMapBlob.(type) {

				case nil:
					buf[jsonMapBlobKey] = EmptyMap

				case []byte:
					if 0 == len(t) {
						buf[jsonMapBlobKey] = EmptyMap
					} else {
						var ob bj.OriginalBlob = t
						fm, e := f(ob)(ctx)
						if nil != e {
							yield(buf, e)
							return
						}
						buf[jsonMapBlobKey] = fm
					}

				case string:
					_, _ = bbuf.WriteString(t) // error is always nil or panic
					var ob bj.OriginalBlob = bbuf.Bytes()
					fm, e := f(ob)(ctx)
					if nil != e {
						yield(buf, e)
						return
					}
					buf[jsonMapBlobKey] = map[string]any(fm)

				default:
					yield(buf, ErrInvalidBlob)
					return

				}

				if !yield(buf, nil) {
					return
				}
			}
		}, nil
	}
}

func (f BlobToFlatMap) BlobKeyToMapsToMaps(
	jsonMapBlobKey string,
) func(iter.Seq2[map[string]any, error]) IO[iter.Seq2[map[string]any, error]] {
	return func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return f.MapsToMaps(original, jsonMapBlobKey)
	}
}
