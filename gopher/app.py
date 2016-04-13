import redis
import json
from gopher import EventConsumer, ServerDataEventProcessor, EventProcessor, QueriesSummaryEventProcessor,\
    TopKEventProcessor, MalformedPacketsEventProcessor
from flask import Flask, Response

app = Flask(__name__)

config = json.load(open("config.json"))  # TODO: Think how to pass configuration file
r = redis.StrictRedis(host=config['redis']['address'], port=config['redis']['port'], db=0)

event_processors = {
    'server_data': ServerDataEventProcessor(r),
    'queries_summary': QueriesSummaryEventProcessor(r, config),
    # 'topk': TopKEventProcessor(r, config),
    'malformed': MalformedPacketsEventProcessor(r, config)
}

for name, event_processor in event_processors.items():
    event_processor.start()
    

def create_sse_response(event_processor: EventProcessor) -> Response:
    def stream():
        ec = EventConsumer()
        event_processor.register_consumer(ec)
        try:
            while True:
                data = ec.get_data()
                yield 'data: %s\n\n' % json.dumps(data)
        except GeneratorExit:
            event_processor.unregister_consumer(ec)

    return Response(stream(),
                    mimetype='text/event-stream',
                    headers={'Access-Control-Allow-Origin': '*'})


@app.route("/sse_data/<name>")
def sse_data(name=None):
    if name != None:
        if name in event_processors:
            return create_sse_response(event_processors[name])
        else:
            return None
    else:
        return None


@app.route("/servData")
def serv_data():
    return create_sse_response(event_processors['server_data'])


@app.route("/geo")
def geo():
    return create_sse_response(event_processors['queries_summary'])


@app.route("/servers_location")
def servers_location():
    return json.dumps(config['servers']) + "\n\n"

