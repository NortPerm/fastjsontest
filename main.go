package main

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fastjson"
	"log"
	"strconv"
	"time"
)

type (
	TraceContext struct {
		Traceparent string `json:"traceparent,omitempty"`
	}

	Event struct {
		ID           string          `json:"id,omitempty"`
		Type         string          `json:"type,omitempty"`
		Name         string          `json:"name,omitempty"`
		Seq          int             `json:"seq"`
		Source       string          `json:"src,omitempty"`
		Destination  string          `json:"dst,omitempty"`
		Offset       int             `json:"offset,omitempty"`
		Param        json.RawMessage `json:"param,omitempty"`
		TraceContext *TraceContext   `json:"traceContext,omitempty"`
	}

	KafkaEvent struct {
		TraceContext *TraceContext `json:"traceContext,omitempty"`
		Message      *Event        `json:"message,omitempty"`
	}
)

const iterations = 1000000
const eventMessage = `
{
	"traceContext": {
		"traceparentAtatus": "",
		"traceparent": "00-148bd7cbb2101d77b49c0c965d091f9e-6939d1b8a77add5e-01",
		"tracestate": ""
	},
	"message": {
		"type": "event",
		"id": "1ede5cba-e2ad-6adb-93cb-56d8a177e38f",
		"name": "ChatEvent",
		"dst": "chat-1eddac8c-40df-67eb-958d-41559d14d163",
		"src": "chat",
		"param": {
			"type": "NewMessageEvent",
			"value": {
				"message": {
					"id": "1ede5cba-e2a8-666f-93cb-e1c6d7cbd2a4",
					"authorId": "1edc1a64-1047-6c28-a501-c7097012ec7f",
					"chatId": "1eddac8c-40df-67eb-958d-41559d14d163",
					"text": "",
					"createdAt": 1682689848,
					"type": "Call",
					"seq": 6,
					"status": "Sent",
					"metadata": {
						"type": "CallMetadata",
						"value": {
							"webinarEventId": "1ede5cba-dc33-64b4-8c02-2124a6bcbb05",
							"status": "Started"
						}
					}
				}
			}
		}
	}
}
`

// need to set Offset, set Seq and extract message
/*

{
	"id": "1ede5cba-e2ad-6adb-93cb-56d8a177e38f",
	"type": "event",
	"name": "ChatEvent",
	"seq": 364,
	"src": "chat",
	"dst": "chat-1eddac8c-40df-67eb-958d-41559d14d163",
	"offset": 12,
	"param": {
		"type": "NewMessageEvent",
		"value": {
			"message": {
				"id": "1ede5cba-e2a8-666f-93cb-e1c6d7cbd2a4",
				"authorId": "1edc1a64-1047-6c28-a501-c7097012ec7f",
				"chatId": "1eddac8c-40df-67eb-958d-41559d14d163",
				"text": "",
				"createdAt": 1682689848,
				"type": "Call",
				"seq": 6,
				"status": "Sent",
				"metadata": {
					"type": "CallMetadata",
					"value": {
						"webinarEventId": "1ede5cba-dc33-64b4-8c02-2124a6bcbb05",
						"status": "Started"
					}
				}
			}
		}
	},
	"traceContext": {
		"traceparent": "00-148bd7cbb2101d77b49c0c965d091f9e-6939d1b8a77add5e-01"
	}
}
*/

func stdSetFields(a []byte, offset, seq int) []byte {
	var km KafkaEvent
	var msg Event
	var trace *TraceContext
	trace = nil
	err := json.Unmarshal(a, &km)
	if err != nil {
		log.Fatal("JSON unmarshal fail")
		return nil
	}
	msg = *km.Message
	trace = km.TraceContext
	msg.TraceContext = trace
	msg.Offset = offset
	msg.Seq = seq
	mrshl, _ := json.Marshal(msg)
	return mrshl
}

func fastSetFields(a []byte, offset, seq int, p *fastjson.Parser) []byte {
	value, err := p.ParseBytes(a)
	if err != nil {
		log.Fatal("JSON unmarshal fail")
		return nil
	}
	event := value.Get("message")

	event.Set("seq", fastjson.MustParse(strconv.Itoa(seq)))
	event.Set("offset", fastjson.MustParse(strconv.Itoa(offset)))
	event.Set("traceContext", value.Get("traceContext"))
	return event.MarshalTo(nil)
}

func main() {
	tmp := []byte(eventMessage)
	offset := 20
	seq := 4
	t := time.Now()
	var res []byte
	for i := 0; i < iterations; i++ {
		res = stdSetFields(tmp, offset, seq)
		offset++
		seq++
	}
	fmt.Println(string(res))
	fmt.Println(time.Since(t))
	parser := &fastjson.Parser{}
	offset = 20
	seq = 4
	t = time.Now()
	for i := 0; i < iterations; i++ {
		res = fastSetFields(tmp, offset, seq, parser)
		offset++
		seq++
	}
	fmt.Println(string(res))
	fmt.Println(time.Since(t))

}
