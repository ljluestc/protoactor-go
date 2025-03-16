package protocb

import (
    "encoding/json"
    "log"
    "reflect"

    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/reflect/protoreflect"
    "google.golang.org/protobuf/reflect/protoregistry"
    goreflect "github.com/goccy/go-reflect"
)

type envelope struct {
    Type       string          `json:"type"`       // reflected message type so we can deserialize back
    Data       json.RawMessage `json:"event"`      // renamed from Message to Data to avoid conflict
    EventIndex int             `json:"eventIndex"` // event index in the event stream
    DocType    string          `json:"doctype"`    // type snapshot or event
}

func NewEnvelope(message proto.Message, doctype string, eventIndex int) *envelope {
    typeName := proto.MessageName(message)
    bytes, err := json.Marshal(message)
    if err != nil {
        log.Fatal(err)
    }
    envelope := &envelope{
        Type:       string(typeName),
        Data:       bytes, // Changed from Message to Data
        EventIndex: eventIndex,
        DocType:    doctype,
    }
    return envelope
}

func (envelope *envelope) Message() proto.Message {
    mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(envelope.Type))
    if err != nil {
        log.Fatal(err)
    }

    // Use goccy/go-reflect instead of reflect for New, but keep reflect.TypeOf
    t := reflect.TypeOf(mt.New().Interface()).Elem()
    rt := goreflect.ToType(t)
    v := goreflect.New(rt).Interface()
    err = json.Unmarshal(envelope.Data, v) // Changed from Message to Data
    if err != nil {
        log.Fatal(err)
    }
    return v.(proto.Message)
}