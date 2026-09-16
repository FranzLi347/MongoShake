package oplog

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// ParseRaw decodes oplog metadata while borrowing CRUD payloads from input.
// The caller owns input and must not modify/reuse it while the log is alive.
// Commands (including applyOps/transactions) and legacy index inserts retain
// their existing decoded representation.
func ParseRaw(input []byte) (*PartialLog, error) {
	raw := bson.Raw(input)
	if err := raw.Validate(); err != nil {
		return nil, err
	}
	elements, err := raw.Elements()
	if err != nil {
		return nil, err
	}
	metadata := make([]byte, 4, 256)
	var object, query bson.Raw
	for _, element := range elements {
		key := element.Key()
		switch key {
		case "o", "o2":
			value := element.Value()
			var doc bson.Raw
			switch value.Type {
			case bsontype.EmbeddedDocument:
				doc = value.Document()
			case bsontype.Null, bsontype.Undefined:
			default:
				return nil, fmt.Errorf("oplog %s must be a document, got %s", key, value.Type)
			}
			if key == "o" {
				object = doc
			} else {
				query = doc
			}
		case "ts", "t", "h", "v", "op", "g", "ns", "uk", "lsid",
			"fromMigrate", "txnNumber", "documentKey", "prevOpTime", "ui", "b", "multiOpType":
			metadata = append(metadata, element...)
		}
	}
	metadata = finishDocument(metadata)
	log := &PartialLog{}
	if err := bson.Unmarshal(metadata, &log.ParsedLog); err != nil {
		return nil, err
	}
	switch log.Operation {
	case "i", "u", "d":
		if !strings.HasSuffix(log.Namespace, ".system.indexes") {
			log.objectRaw, log.queryRaw = object, query
			return log, nil
		}
	}
	return log, bson.Unmarshal(input, &log.ParsedLog)
}

func finishDocument(doc []byte) []byte {
	doc = append(doc, 0)
	binary.LittleEndian.PutUint32(doc[:4], uint32(len(doc)))
	return doc
}

// ObjectValue and QueryValue return a document accepted by the MongoDB driver.
// Decoded fields take precedence, so explicit transformations cannot serialize
// stale raw payloads. Treat returned raw documents as immutable.
func (log *ParsedLog) ObjectValue() interface{} {
	if log.Object != nil || log.objectRaw == nil {
		return log.Object
	}
	return log.objectRaw
}

func (log *ParsedLog) QueryValue() interface{} {
	if log.Query != nil || log.queryRaw == nil {
		return log.Query
	}
	return log.queryRaw
}

// MaterializeObject is required before code that mutates Object in place.
func (log *ParsedLog) MaterializeObject() error {
	if log.Object == nil && log.objectRaw != nil {
		if err := bson.Unmarshal(log.objectRaw, &log.Object); err != nil {
			return err
		}
	}
	log.objectRaw = nil
	return nil
}

// LookupDocument decodes only the requested value. It preserves BSON types and
// distinguishes a present null value from a missing field, including for _id.
func LookupDocument(doc interface{}, keys ...string) (interface{}, bool) {
	if raw, ok := doc.(bson.Raw); ok {
		value, err := raw.LookupErr(keys...)
		if err != nil {
			return nil, false
		}
		var result interface{}
		if err := value.Unmarshal(&result); err != nil {
			return nil, false
		}
		return result, true
	}
	current := doc
	for _, key := range keys {
		d, ok := current.(bson.D)
		if !ok {
			return nil, false
		}
		current, ok = getValueFromBsonD(d, key)
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func (log *ParsedLog) ObjectKey(key string) interface{} {
	if key == "" {
		key = PrimaryKey
	}
	value, _ := LookupDocument(log.ObjectValue(), key)
	return value
}

// IndexValue follows the collision matrix's exact-key-before-dotted-path
// semantics, looking under $set when present. Only the selected value is decoded.
func (log *ParsedLog) IndexValue(path string) interface{} {
	doc := log.ObjectValue()
	switch object := doc.(type) {
	case bson.Raw:
		if set, ok := object.Lookup("$set").DocumentOK(); ok {
			doc = set
		}
	case bson.D:
		if set, ok := GetKey(object, "$set").(bson.D); ok {
			doc = set
		}
	}
	value, found := LookupDocument(doc, path)
	if !found {
		value, _ = LookupDocument(doc, strings.Split(path, ".")...)
	}
	if object, ok := value.(bson.D); ok {
		value, _ = ConvertBsonD2M(object)
	}
	return value
}

func (log *ParsedLog) ObjectHasPrefix(prefix string) bool {
	if raw, ok := log.ObjectValue().(bson.Raw); ok {
		// Raw is validated by ParseRaw. Walk element boundaries without building
		// a slice proportional to the number of document fields.
		remaining := []byte(raw[4 : len(raw)-1])
		for len(remaining) > 0 {
			element, rest, ok := bsoncore.ReadElement(remaining)
			if !ok {
				return false
			}
			if strings.HasPrefix(element.Key(), prefix) {
				return true
			}
			remaining = rest
		}
		return false
	}
	return FindFiledPrefix(log.Object, prefix)
}

// UpdateValue removes the internal $v marker without decoding v1 modifiers or
// replacement documents. V2 diffs require the existing conversion at execution
// time; its decoded input is temporary and is not retained in queued logs.
func (log *ParsedLog) UpdateValue() (interface{}, error) {
	if version, ok := log.ObjectKey("$v").(int32); ok && version == 2 {
		object := log.Object
		if object == nil && log.objectRaw != nil {
			if err := bson.Unmarshal(log.objectRaw, &object); err != nil {
				return nil, err
			}
		}
		return DiffUpdateOplogToNormal(object)
	}
	if raw, ok := log.ObjectValue().(bson.Raw); ok {
		if _, err := raw.LookupErr("$v"); err != nil {
			return raw, nil
		}
		out := make([]byte, 4, len(raw))
		remaining := []byte(raw[4 : len(raw)-1])
		for len(remaining) > 0 {
			element, rest, ok := bsoncore.ReadElement(remaining)
			if !ok {
				return nil, fmt.Errorf("invalid update document")
			}
			if element.Key() != "$v" {
				out = append(out, element...)
			}
			remaining = rest
		}
		return bson.Raw(finishDocument(out)), nil
	}
	// Do not modify the original: retry/applyOps still needs the source oplog.
	out := make(bson.D, 0, len(log.Object))
	for _, element := range log.Object {
		if element.Key != "$v" {
			out = append(out, element)
		}
	}
	return out, nil
}

// encodedLog prevents recursive calls to the BSON/JSON marshal methods.
type encodedLog ParsedLog

func (log ParsedLog) MarshalBSON() ([]byte, error) {
	encoded, err := bson.Marshal(encodedLog(log))
	if err != nil || (log.objectRaw == nil && log.queryRaw == nil) {
		return encoded, err
	}
	elements, err := bson.Raw(encoded).Elements()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4, len(encoded)+len(log.objectRaw)+len(log.queryRaw))
	for _, element := range elements {
		switch {
		case element.Key() == "o" && log.Object == nil && log.objectRaw != nil:
			out = bsoncore.AppendDocumentElement(out, "o", log.objectRaw)
		case element.Key() == "o2" && log.Query == nil && log.queryRaw != nil:
			out = bsoncore.AppendDocumentElement(out, "o2", log.queryRaw)
		default:
			out = append(out, element...)
		}
	}
	return finishDocument(out), nil
}

func (log ParsedLog) MarshalJSON() ([]byte, error) {
	// Decode a copy for the existing JSON representation, never cache an expanded
	// tree as a side effect of logging or concurrently reading a queued oplog.
	if err := log.MaterializeObject(); err != nil {
		return nil, err
	}
	if log.Query == nil && log.queryRaw != nil {
		if err := bson.Unmarshal(log.queryRaw, &log.Query); err != nil {
			return nil, err
		}
	}
	return json.Marshal(encodedLog(log))
}
