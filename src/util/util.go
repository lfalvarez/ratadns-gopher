package util
import (
	"sync"
	"encoding/hex"
	"net"
	"fmt"
)

func SynchronizeNbOfClients(lock *sync.Mutex, numberOfClients *int, connectedClients chan bool, controlChannels ... chan bool){
	for {

		connected := <- connectedClients
		lock.Lock()
		if connected { // If clients connects
			*numberOfClients++
		} else {
			*numberOfClients--
		}
		lock.Unlock()
		if *numberOfClients == 1 {
			for _, channel:= range controlChannels{
				channel <- connected
			}
		}

	}
}

func HexToIp(hexIp string) string {
	if len(hexIp) == 8 {

		ipBytes, err := hex.DecodeString(hexIp)
		if err != nil { panic(err) }
		ip := net.IPv4(ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3])
		return ip.String()
	} else if len(hexIp) == 32 {
		panic("ipv6 not supported (yet)")
	} else if len(hexIp) == 7 {
		ipBytes, err := hex.DecodeString("0"+hexIp)
		if err != nil { panic(err) }
		ip := net.IPv4(ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3])
		return ip.String()
	}else {
		fmt.Println(hexIp)
		panic("hexIp length not correct")
	}
}