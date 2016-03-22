import redis
from gopher import EventConsumer, ServerDataEventProcessor, EventProcessor
from flask import Flask, Response

app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=0)
serverData = ServerDataEventProcessor(r)
serverData.start()

def create_stream(event_processor: EventProcessor):
    def stream():
        ec = EventConsumer()
        event_processor.register_consumer(ec)
        try:
            while True:
                data = ec.get_data()
                yield 'data: %s\n\n' % data
        except GeneratorExit:
            event_processor.unregister_consumer(ec)

    return stream


@app.route("/servData")
def serv_data():
    return Response(create_stream(serverData)(), mimetype='text/event-stream')

if __name__ == "__main__":
    app.run()