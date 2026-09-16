package executor

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/transform"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func lazyRecord(t testing.TB, op string, object bson.D) (*OplogRecord, []byte) {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{"ts", primitive.Timestamp{T: 100, I: 1}}, {"op", op}, {"ns", "source.coll"},
		{"o", object}, {"o2", bson.D{{"_id", int32(1)}}},
	})
	require.NoError(t, err)
	log, err := oplog.ParseRaw(raw)
	require.NoError(t, err)
	return &OplogRecord{original: &PartialLogWithCallback{partialLog: log}}, raw
}

func TestLazyWriters(t *testing.T) {
	originalOptions := conf.Options
	defer func() { conf.Options = originalOptions }()
	conf.Options.IncrSyncBypassDocumentValidation = false
	conf.Options.IncrSyncExecutorDupKeyStrategy = "ignore"
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	cases := []struct {
		name string
		op string
		object bson.D
		expected bson.D
	}{
		{"insert", "i", bson.D{{"_id", int32(1)}, {"value", bson.D{{"a", "payload"}}}}, nil},
		{"delete", "d", bson.D{{"_id", int32(1)}}, nil},
		{"replace", "u", bson.D{{"_id", int32(1)}, {"x", int32(2)}}, bson.D{{"_id", int32(1)}, {"x", int32(2)}}},
		{"v1", "u", bson.D{{"$v", int32(1)}, {"$set", bson.D{{"x", int32(2)}}}}, bson.D{{"$set", bson.D{{"x", int32(2)}}}}},
		{"v2", "u", bson.D{{"$v", int32(2)}, {"diff", bson.D{{"u", bson.D{{"x", int32(2)}}}}}}, bson.D{{"$set", bson.D{{"x", int32(2)}}}}},
		{"updateOnInsert", "i", bson.D{{"_id", int32(1)}, {"x", int32(2)}}, nil},
	}
	for _, kind := range []string{"bulk", "single", "command"} {
		for _, tc := range cases {
			mt.Run(kind+"/"+tc.name, func(mt *mtest.T) {
				conn := &utils.MongoCommunityConn{Client: mt.Client}
				var writer BasicWriter
				switch kind {
				case "bulk": writer = &BulkWriter{conn: conn}
				case "single": writer = &SingleWriter{conn: conn}
				case "command": writer = &CommandWriter{conn: conn}
				}
				record, raw := lazyRecord(mt, tc.op, tc.object)
				before := bytes.Clone(raw)
				log := record.original.partialLog
				transformPartialLog(log, transform.NewNamespaceTransform([]string{"source.coll:target.renamed"}), false)
				require.Equal(mt, "target.renamed", log.Namespace)
				require.IsType(mt, bson.Raw{}, log.ObjectValue())
				mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: int32(1)}, bson.E{Key: "nModified", Value: int32(1)}))
				records := []*OplogRecord{record}
				var err error
				switch {
				case tc.name == "updateOnInsert": err = writer.doUpdateOnInsert("target", "renamed", bson.E{}, records, false)
				case tc.op == "i": err = writer.doInsert("target", "renamed", bson.E{}, records, false)
				case tc.op == "u": err = writer.doUpdate("target", "renamed", bson.E{}, records, false)
				case tc.op == "d": err = writer.doDelete("target", "renamed", bson.E{}, records)
				}
				require.NoError(mt, err)
				event := mt.GetStartedEvent()
				require.NotNil(mt, event)
				require.Equal(mt, "target", event.DatabaseName)
				require.Equal(mt, "renamed", event.Command.Lookup(event.CommandName).StringValue())
				var sent bson.Raw
				expected := tc.object
				switch event.CommandName {
				case "insert": sent = event.Command.Lookup("documents").Array().Index(0).Value().Document()
				case "delete": sent = event.Command.Lookup("deletes").Array().Index(0).Value().Document().Lookup("q").Document()
				case "update":
					update := event.Command.Lookup("updates").Array().Index(0).Value().Document()
					require.Equal(mt, int32(1), update.Lookup("q", "_id").Int32())
					sent = update.Lookup("u").Document()
					if tc.name == "updateOnInsert" {
						if kind != "command" { expected = bson.D{{"$set", tc.object}} }
					} else { expected = tc.expected }
				default: mt.Fatalf("unexpected command %s", event.CommandName)
				}
				expectedBytes, err := bson.Marshal(expected)
				require.NoError(mt, err)
				require.Equal(mt, expectedBytes, []byte(sent))
				require.Nil(mt, log.Object)
				require.Nil(mt, log.Query)
				require.Equal(mt, before, raw)
			})
		}
	}
}

func TestLazyNamespaceAndDBRef(t *testing.T) {
	for _, ns := range []string{"source.coll", "source.system.buckets.coll"} {
		raw, err := bson.Marshal(bson.D{{"op", "i"}, {"ns", ns}, {"o", bson.D{{"_id", int32(1)}}}})
		require.NoError(t, err)
		log, err := oplog.ParseRaw(raw)
		require.NoError(t, err)
		transformPartialLog(log, transform.NewNamespaceTransform([]string{"source.coll:target.renamed"}), false)
		expected := "target.renamed"
		if ns != "source.coll" { expected = "target.system.buckets.renamed" }
		require.Equal(t, expected, log.Namespace)
		require.Nil(t, log.Object)
		encoded := oplog.LogEntryEncode([]*oplog.GenericOplog{{Parsed: log}})[0]
		require.Equal(t, expected, bson.Raw(encoded).Lookup("ns").StringValue())
		require.Equal(t, bson.Raw(raw).Lookup("o").Value, bson.Raw(encoded).Lookup("o").Value)
	}
	record, _ := lazyRecord(t, "i", bson.D{{"$ref", "coll"}, {"$id", int32(1)}, {"$db", "source"}})
	log := record.original.partialLog
	transformPartialLog(log, transform.NewNamespaceTransform([]string{"source:target"}), true)
	require.NotNil(t, log.Object)
	require.Equal(t, "target", oplog.GetKey(log.Object, "$db"))
	encoded, err := bson.Marshal(log)
	require.NoError(t, err)
	require.Equal(t, "target", bson.Raw(encoded).Lookup("o", "$db").StringValue())
}

func TestLazyCollisionAndConflictFilter(t *testing.T) {
	object := bson.D{{"_id", int32(1)}, {"nested", bson.D{{"key", "value"}}}, {"nullable", nil}}
	for _, op := range []string{"i", "u"} {
		payload := object
		if op == "u" { payload = bson.D{{"$set", object}} }
		record, raw := lazyRecord(t, op, payload)
		log := record.original.partialLog
		log.UniqueIndexes = bson.M{"nested.key|nullable": nil}
		var eager oplog.PartialLog
		require.NoError(t, bson.Unmarshal(raw, &eager.ParsedLog))
		eager.UniqueIndexes = bson.M{"nested.key|nullable": nil}
		fillupOperationValues(record.original)
		fillupOperationValues(&PartialLogWithCallback{partialLog: &eager})
		require.Equal(t, eager.UniqueIndexes, log.UniqueIndexes)
		require.Equal(t, eager.UniqueIndexesUpdates, log.UniqueIndexesUpdates)
		require.Nil(t, log.Object)
	}
	record, _ := lazyRecord(t, "i", object)
	for _, key := range []string{"nested.key", "nullable", "missing"} {
		v, found := getFieldValue(record.original.partialLog.ObjectValue(), key)
		want, wantFound := getFieldValue(object, key)
		require.Equal(t, want, v)
		require.Equal(t, wantFound, found)
	}
}
