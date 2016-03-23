import redis
import json
from gopher import EventConsumer, ServerDataEventProcessor, EventProcessor, QueriesSummaryEventProcessor
from flask import Flask, Response

app = Flask(__name__)
#r = redis.StrictRedis(host='localhost', port=6379, db=0)
r = redis.StrictRedis(host='172.17.66.212', port=6379, db=0)
serverData = ServerDataEventProcessor(r)
serverData.start()

queriesSummary = QueriesSummaryEventProcessor(r)
queriesSummary.start()

def create_sse_response(event_processor: EventProcessor) -> Response:
    def stream():
        ec = EventConsumer()
        event_processor.register_consumer(ec)
        try:
            while True:
                data = ec.get_data()
                if data != None:
                    yield 'data: %s\n\n' % json.dumps(data)
        except GeneratorExit:
            event_processor.unregister_consumer(ec)

    return Response(stream(),
                    mimetype='text/event-stream',
                    headers={
                        'Access-Control-Allow-Origin': '*'
                    })


@app.route("/servData")
def serv_data():
    return create_sse_response(serverData)

@app.route("/geo")
def geo():
    return create_sse_response(queriesSummary)

if __name__ == "__main__":
    app.run(port=8080)