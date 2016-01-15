package main

import (
	"testing"
	"fmt"
	"./util"
)

type hexIpTestPair struct {
	hexIp string
	ip    string
}

var ipsTests = []hexIpTestPair{
	{"8080808", "8.8.8.8"},
	{"7F000001", "127.0.0.1"},
	{"C8070681", "200.7.6.129"},
	{"AC1E4135", "172.30.65.53"},
}

func TestHexToIp(t *testing.T) {
	for _, pair := range ipsTests {
		v := util.HexToIp(pair.hexIp)
		if v != pair.ip {
			t.Error("For", pair.hexIp, "expected", pair.ip, "got", v, )
		}
	}
}

type unmarshalTestPair struct {
	bytes []byte
	msg   Message
}

var testMap1 = map[string][]string{
	"f":{"jppdrm.cl."},
}

var testMap2 = map[string][]string{
	"2b":{"hotelalbamar.cl."},
}

var testMap3 = map[string][]string{
	"f":{"virutexilko.cl."},
}

var testMap4 = map[string][]string{
	"1":{"collect.cl."},
}


var mySummary = &QueriesSummary{
	{Queries:testMap1, Ip:"c84902b3"},
	{Queries:testMap2, Ip:"bea00034"},
	{Queries:testMap3, Ip:"c849061c"},
	{Queries:testMap4, Ip:"c80c18bb"},
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
	{[]byte(jsonQueriesSummary), Message{ServerId:"blanco", Type:"QueriesSummary", TimeStamp:1452810394, Payload:mySummary}},
}

func TestUnmarshalJSON(t *testing.T) {
	for _, pair := range unmarshalTest {
		var msg Message

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
		for i := range *msg.Payload.(*QueriesSummary){
			if fmt.Sprint((*msg.Payload.(*QueriesSummary))[i]) != fmt.Sprint((*pair.msg.Payload.(*QueriesSummary))[i]) {
				t.Error("The Payload of the messages aren't equal",
					msg.Payload.(*QueriesSummary),
					pair.msg.Payload.(*QueriesSummary))
			}
		}
	}

}
