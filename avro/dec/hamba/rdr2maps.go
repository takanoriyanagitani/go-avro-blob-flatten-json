package dec

import (
	"bufio"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	bj "github.com/takanoriyanagitani/go-avro-blob-flatten-json"
	. "github.com/takanoriyanagitani/go-avro-blob-flatten-json/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var br io.Reader = bufio.NewReader(rdr)

		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(nil, e)
			return
		}

		var buf map[string]any
		for dec.HasNext() {
			clear(buf)
			e := dec.Decode(&buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

func ConfigToOpts(c bj.DecodeConfig) []ho.DecoderFunc {
	var inputBlobSizeMax int = c.InputBlobSizeMax
	var hcfg ha.Config
	hcfg.MaxByteSliceSize = inputBlobSizeMax
	var hapi ha.API = hcfg.Freeze()
	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg bj.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)
	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func StdinToMaps(
	cfg bj.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

func ConfigToStdinToMaps(
	cfg bj.DecodeConfig,
) IO[iter.Seq2[map[string]any, error]] {
	return OfFn(
		func() iter.Seq2[map[string]any, error] { return StdinToMaps(cfg) },
	)
}

var StdinToMapsDefault IO[iter.
	Seq2[map[string]any, error]] = ConfigToStdinToMaps(
	bj.DecodeConfigDefault,
)
