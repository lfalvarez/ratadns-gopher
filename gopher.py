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

def create_stream(event_processor: EventProcessor):
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

    return stream


@app.route("/servData")
def serv_data():
    return Response(create_stream(serverData)(), mimetype='text/event-stream')

@app.route("/geo")
def geo():
    return Response(create_stream(queriesSummary)(), mimetype='text/event-stream')

if __name__ == "__main__":
    app.run()