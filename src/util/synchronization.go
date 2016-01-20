package util
import (
	"sync"
)

func SynchronizeNbOfClients(lock *sync.Mutex, numberOfClients *int, connectedClients chan bool, controlChannels ... chan bool){
	for {
		connected := <- connectedClients
		lock.Lock()
		if connected { // If clients connects
			*numberOfClients++

		} else { //If client disconnects
			*numberOfClients--
		}
		lock.Unlock()
		if connected && *numberOfClients == 1 {
			for _, channel:= range controlChannels{
				channel <- connected
			}
		}
	}
}
