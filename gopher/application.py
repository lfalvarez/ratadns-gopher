import redis
import json
from flask import Flask, Response
from gopher import EventConsumer, \
        ServerDataEventProcessor, \
        EventProcessor, \
        QueriesSummaryEventProcessor, \
        TopQNamesEventProcessor, \
        ServerDataV2EventProcessor, \
        DataSortedByQTypeEventProcessor


def create_app(config_file):
    app = Flask(__name__)

    event_processors = {
        'server_data': ServerDataEventProcessor,
        'queries_summary': QueriesSummaryEventProcessor,
        'top_qnames': TopQNamesEventProcessor,
        'server_data_v2': ServerDataV2EventProcessor,
        'queries_by_qtype': DataSortedByQTypeEventProcessor
    }

    config = json.load(open(config_file)) 
    r = redis.StrictRedis(host=config['redis']['address'], port=config['redis']['port'], db=0)
    r.flushall()

    active_event_processors = {ep_name: event_processors[ep_name](r, config) for ep_name in
                               config['active_event_processors']}

    for name, event_processor in active_event_processors.items():
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
                event_processor.deregister_consumer(ec)

        return Response(stream(),
                        mimetype='text/event-stream',
                        headers={'Access-Control-Allow-Origin': '*'})

    @app.route("/{}/<name>".format(config["sse_route"]))
    def sse_data(name=None):
        if name is not None:
            if name in active_event_processors:
                return create_sse_response(active_event_processors[name])
            else:
                return None
        else:
            return None

    @app.route("/{}".format(config["servers_location_route"]))
    def servers_location():
        return json.dumps(config['servers_info']) + "\n\n"

    return app
