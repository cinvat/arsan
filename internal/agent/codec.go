package agent

import (
	"encoding/json"
	"sync"

	"google.golang.org/grpc/encoding"
)

// Messages are plain structs, not protobuf, so gRPC's default proto codec
// rejects them ("want proto.Message"). This codec marshals them as JSON
// instead. Both ends live in this package, so both ends agree.
//
// It is registered explicitly from NewClient and RegisterServer rather than
// init, so importing the package never changes global gRPC state as a side
// effect.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return "json" }

var registerCodecOnce sync.Once

func registerCodec() {
	registerCodecOnce.Do(func() { encoding.RegisterCodec(jsonCodec{}) })
}
