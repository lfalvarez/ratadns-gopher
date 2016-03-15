package tests

import (
	"testing"
	"fmt"
	"../servers"
)

type unmarshalTestPair struct {
	bytes []byte
	msg   servers.Message
}

var query1 = map[string][]string{
	"f":{"jppdrm.cl."},
}

var query2 = map[string][]string{
	"2b":{"hotelalbamar.cl."},
}

var query3 = map[string][]string{
	"f":{"virutexilko.cl."},
}

var query4 = map[string][]string{
	"1":{"collect.cl."},
}


var queriesSummary = &servers.QueriesSummary{
	{Queries:query1, Ip:"c84902b3"},
	{Queries:query2, Ip:"bea00034"},
	{Queries:query3, Ip:"c849061c"},
	{Queries:query4, Ip:"c80c18bb"},
}
var jsonQueriesSummary =
`{
  "serverId": "blanco",
  "type": "QueriesSummary",
  "data": [
    {
      "ip": "c84902b3",
      "queries": {
        "f": [
          "jppdrm.cl."
        ]
      }
    },
    {
      "ip": "bea00034",
      "queries": {
        "2b": [
          "hotelalbamar.cl."
        ]
      }
    },
    {
      "ip": "c849061c",
      "queries": {
        "f": [
          "virutexilko.cl."
        ]
      }
    },
    {
      "ip": "c80c18bb",
      "queries": {
        "1": [
          "collect.cl."
        ]
      }
    }
  ],
  "timeStamp": 1452810394
}`
var unmarshalTest = []unmarshalTestPair{
	{[]byte(jsonQueriesSummary), servers.Message{ServerId:"blanco", Type:"QueriesSummary", TimeStamp:1452810394, Payload:queriesSummary}},
}

func TestUnmarshalJSON(t *testing.T) {
	for _, pair := range unmarshalTest {
		var msg servers.Message
		err := msg.UnmarshalJSON(pair.bytes)
		if err != nil {t.Error("An error ocurred while unmarshaling", err)}

		if msg.ServerId != pair.msg.ServerId {
			t.Error("The serverId of the messages aren't equal", msg.ServerId, pair.msg.ServerId)
		}
		if msg.Type != pair.msg.Type {
			t.Error("The MsgTypes of the messages aren't equal", msg.Type, pair.msg.Type)
		}
		if msg.TimeStamp != pair.msg.TimeStamp {
			t.Error("The timestamps of the messages aren't equal", msg.TimeStamp, pair.msg.TimeStamp)
		}
		for i := range *msg.Payload.(*servers.QueriesSummary){
			if fmt.Sprint((*msg.Payload.(*servers.QueriesSummary))[i]) != fmt.Sprint((*pair.msg.Payload.(*servers.QueriesSummary))[i]) {
				t.Error("The Payload of the messages aren't equal",
					msg.Payload.(*servers.QueriesSummary),
					pair.msg.Payload.(*servers.QueriesSummary))
			}
		}
	}
}
