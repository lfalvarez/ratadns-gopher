package tests
import (
	"testing"
	"ratadns-gopher/util"
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
