package servers

import (
	"gopkg.in/redis.v3"
	"sort"
	"time"
	"strconv"
	"encoding/json"
	"reflect"
	"gopkg.in/natefinch/lumberjack.v2"
	"ratadns-gopher/util"
)

//TopKEvent function subscribe to "topk" and "QueriesWithUnderscoredName" channels, obtains configuration information,
//and launches funEventManagerctions to obtain the message of the redis channels, spread that message, process it and write the
//processed message in a HTML5 SSE.
func TopKEvent(channel chan[]byte, client *redis.Client, l *lumberjack.Logger, c util.Configuration) {
	topk, err := client.Subscribe("topk")
	if err != nil {
		l.Write([]byte("["+time.Now().String()+"]"+err.Error()+"\n"))
	}
	redisWriter := redis.NewClient(&redis.Options{
		Addr:    c.Redis.Address,
	})
	malformed, err := client.Subscribe("QueriesWithUnderscoredName")
	if err != nil {
		l.Write([]byte("["+time.Now().String()+"]"+err.Error()+"\n"))
	}

	times := c.TopK.Times

	script := "local old_jsons = redis.call('zrangebyscore', KEYS[1], '-inf' , ARGV[1]);" +
	"redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1]);" +
	"return old_jsons;" // TODO: Exportar a archivo de configuración

	nameCountChannel := make(chan QueryCounterMsg)
	go orderTopK(topk, nameCountChannel)
	nameCountChannels := make([]chan QueryCounterMsg, len(times))
	go spreadMessage(nameCountChannel, nameCountChannels)
	malformedChannel := make(chan QueryCounterMsg)
	go orderMalformed(malformed, malformedChannel)
	malformedChannels := make([]chan QueryCounterMsg, len(times))
	go spreadMessage(malformedChannel, malformedChannels)

	//for every different time span, launch the function that process the information in that time span.
	for i, seconds := range times {
		nameCountChannels[i] = make(chan QueryCounterMsg)
		go obtainTopK(seconds, script, "nameCount", nameCountChannels[i], redisWriter, channel, l)
		malformedChannels[i] = make(chan QueryCounterMsg)
		go obtainTopK(seconds, script, "malformed", malformedChannels[i], redisWriter, channel, l)
	}
}

func orderMalformed(malformed *redis.PubSub, channel chan QueryCounterMsg) (err error){
	for {
		jsonMsg, err := malformed.ReceiveMessage()
		if err != nil {
			return err
		}
		var malformedMsg Message
		err = malformedMsg.UnmarshalJSON([]byte(jsonMsg.Payload))
		if err != nil {
			return err
		}
		orderedValues := make(QueriesCounter, len(*malformedMsg.Payload.(*QueriesWithUnderscoredName)))
		counter := 0
		for key, query := range *malformedMsg.Payload.(*QueriesWithUnderscoredName) {
			orderedValues[counter] = QueryCounter{key, len(query)}
			counter++
		}
		sort.Sort(sort.Reverse(orderedValues))
		channel <- QueryCounterMsg{orderedValues, malformedMsg.ServerId}
	}
}
//FIXME: orderMalformed adn orderTopK are the same but the type of the payload. A refactor would be perfect.
//orderTopKd receives messages of the channel "topk", unmarshall the message,
//order it in a decreasing way and then send the object to a channel so it is spread later.
func orderTopK(topk *redis.PubSub, channel chan QueryCounterMsg) (err error){
	for {
		jsonMsg, err := topk.ReceiveMessage()
		if err != nil {
			return err
		}
		var msg Message
		err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
		if err != nil {
			return err
		}
		orderedValues := make(QueriesCounter, len(*msg.Payload.(*QueryNameCounter)))
		counter := 0
		for value, i := range *msg.Payload.(*QueryNameCounter) {
			orderedValues[counter] = QueryCounter{value, i}
			counter++
		}
		sort.Sort(sort.Reverse(orderedValues))
		channel <- QueryCounterMsg{orderedValues, msg.ServerId}
	}
}

////orderMalformed receives messages of the channel "QueriesWithUnderscoredNames", unmarshall the message,
////order it in a decreasing way and then send the object to a channel so it is spread later.
//func orderMalformed(malformed *redis.PubSub, channel chan QueryCounterMsg) (err error){
//	var msg Message
//	err = order(malformed,channel,*msg.Payload.(*QueriesWithUnderscoredName),msg)
//
//	return err
//}
//
////orderTopKd receives messages of the channel "topk", unmarshall the message,
////order it in a decreasing way and then send the object to a channel so it is spread later.
//func orderTopK(topk *redis.PubSub, channel chan QueryCounterMsg) (err error){
//	var msg Message
//	err = order(topk,channel,*msg.Payload.(*QueryNameCounter),msg)
//
//	return err
//}
//
//func order(pub *redis.PubSub, channel chan QueryCounterMsg, payloadType QueryMap, msg Message) (err error){
//	for {
//		jsonMsg, err := pub.ReceiveMessage()
//		if err != nil {
//			return err
//		}
//
//		err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
//		if err != nil {
//			return err
//		}
//		orderedValues := make(QueriesCounter, len(payloadType))
//		counter := 0
//		for value, i := range payloadType {
//			orderedValues[counter] = QueryCounter{value, i}
//			counter++
//		}
//		sort.Sort(sort.Reverse(orderedValues))
//		channel <- QueryCounterMsg{orderedValues, msg.ServerId}
//	}
//}

//spreadMessage get a message of a channel of QueryCounterMsg and spread it to a slice of channels of QueryCounterMsg .
func spreadMessage(channel chan QueryCounterMsg, channels []chan QueryCounterMsg) {
	for {
		msg := <-channel
		for i := range channels {
			channels[i] <- msg
		}
	}
}

//QueryCounterMsg struct that has a ordered QueriesCounter object and the id of the server that did those request.
type QueryCounterMsg struct {
	qc  QueriesCounter
	serverId string
}

//obtainTopK function that receives a QueryCounterMsg from a channel, saves the moment when the values are added,
//increase the times a url was called, then retrieves the values that are out of the span of time, decrease the times
//those url was called and writes to and sse channel the top k valuesof the redis channel.
//TODO: make that the functioon writes the top K, not only the top 5.
func obtainTopK(seconds string, script string, name string, channel chan QueryCounterMsg, redisWriter *redis.Client, outputChannel chan []byte, l *lumberjack.Logger) {
	for {
		qcm := <-channel
		orderedValues := qcm.qc
		serverId := qcm.serverId
		historicChannel := "historicNameCounts:" + serverId + ":" + seconds//TODO: obtain the channel names of somewhere else, to create the string only one time
		serverChannel := name + ":" + serverId + ":" + seconds //TODO: use templates
		globalChannel := name + ":GLOBAL:" + serverId + ":" + seconds
		multi, err := redisWriter.Watch(historicChannel,
			serverChannel,
			globalChannel)
		if manageError(err, l) {
			continue
		}
		now := float64(time.Now().UnixNano() / 1000000)
		zAdd := redisWriter.ZAdd(historicChannel, redis.Z{now, orderedValues})
		if manageError(zAdd.Err(), l) {
			continue
		}
		for i := 0; i < len(orderedValues); i++ {
			zIncrBy(multi, serverChannel, orderedValues[i], l)
			zIncrBy(multi, globalChannel, orderedValues[i], l)
		}
		execMulti(multi, l)
		floatSeconds, err := strconv.ParseFloat(seconds, 64)
		if manageError(err, l) {
			continue
		}
		timeRange := strconv.FormatFloat(now - floatSeconds * 1000, 'g', -1, 64)
		scriptEval := redisWriter.Eval(script, []string{historicChannel}, []string{timeRange})
		if manageError(scriptEval.Err(), l) {
			continue
		}
		s := scriptEval.Val().([]interface{})
		for i := 0; i < len(s); i++ {
			var oldQueries []interface{}
			err = json.Unmarshal([]byte(s[i].(string)), &oldQueries) //FIXME: check why the unmarshall fails so much
			// json: cannot unmarshal object into Go value of type []interface {}
			if manageError(err, l) {
				continue
			}
			for j := 0; j < len(oldQueries); j++ {
				url := reflect.ValueOf(oldQueries[j]).Index(0).Interface().(string)
				counter := -1 * reflect.ValueOf(oldQueries[j]).Index(1).Interface().(float64)
				serverZIncrBy := multi.ZIncrBy(serverChannel, counter, url)
				if manageError(serverZIncrBy.Err(), l) {
					continue
				}
				globalZIncrBy := multi.ZIncrBy(globalChannel, counter, url)
				if manageError(globalZIncrBy.Err(), l) {
					continue
				}
			}
			multi.ZRemRangeByScore(serverChannel, "-inf", "0")
			multi.ZRemRangeByScore(globalChannel, "-inf", "0")
		}
		execMulti(multi, l)
		serverTopK := multi.ZRevRangeWithScores(serverChannel, 0, 4)
		if manageError(serverTopK.Err(), l) {
			continue
		}
		globalTopK := multi.ZRevRangeWithScores(globalChannel, 0, 4)
		if manageError(globalTopK.Err(), l) {
			continue
		}
		execMulti(multi, l)
		outputChannel <- []byte(getOutputMessage(serverTopK.Val(), serverId, name, seconds, l))
		outputChannel <- []byte(getOutputMessage(globalTopK.Val(), "GLOBAL", name, seconds, l))
		multi.Close()
	}
}

//zIncrBy add the ZIncrBy call to the multi redis channel.
func zIncrBy(multi *redis.Multi, channel string, qc QueryCounter, l *lumberjack.Logger) {
	incrBy := multi.ZIncrBy(channel, float64(qc.Counter), qc.Query)
	if incrBy.Err() != nil {
		l.Write([]byte("["+time.Now().String()+"]"+incrBy.Err().Error()+"\n"))
	}
}

//getOutputMessage create the message to send to the HTML5 SSE.
func getOutputMessage(values []redis.Z, serverId string, name string, seconds string, l *lumberjack.Logger) []byte {
	serverTopKValues := make([][]string, len(values))
	for i, value := range values {
		serverTopKValues[i] = []string{value.Member.(string), strconv.FormatFloat(value.Score, 'f', -1, 64)}
	}
	serverTopkMessage := Message{ServerId:serverId, Type: name + "TopK:" + serverId + ":" + seconds,
		TimeStamp:time.Now().UnixNano() / 1000000, Payload:serverTopKValues}
	outputMessage, err := serverTopkMessage.MarshalJSON()
	if err != nil {
		l.Write([]byte("["+time.Now().String()+"]"+err.Error()+"\n"))
	}
	return outputMessage
}

//execMulti executes the instructions of the multi channel. Logs the errors if they appear.
func execMulti(multi *redis.Multi, l *lumberjack.Logger) {
	if _, err := multi.Exec(func() error {
		return nil
	}); err != nil {
		l.Write([]byte("["+time.Now().String()+"]"+err.Error()+"\n"))
	}
}

//manageError logs the error err if it's not nil. Returns true if the error is different from nil so the program
//can continue without interruptions.
func manageError(err error, l *lumberjack.Logger) bool {
	if err != nil {
		l.Write([]byte("["+time.Now().String()+"]"+err.Error()+"\n"))
		return true
	}
	return false
}