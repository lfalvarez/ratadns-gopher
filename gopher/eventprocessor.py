import redis
import threading
import json

class EventProcessor(threading.Thread):
    """One event processor for each URL"""

    def __init__(self):
        threading.Thread.__init__(self)
        self.consumers = []

    def register_consumer(self, event_consumer):
        self.consumers.append(event_consumer)

    def unregister_consumer(self, event_consumer):
        self.consumers.remove(event_consumer)


class ServerDataEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis):
        super(ServerDataEventProcessor, self).__init__()
        self.pubsub = r.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe("QueriesPerSecond")
        self.pubsub.subscribe("AnswersPerSecond")


    def run(self):
        for serialized_item in self.pubsub.listen():
            item = json.loads(str(serialized_item['data'], "utf-8"))
            for consumer in self.consumers:
                consumer.consume(item) # Simply pass the item to the consumer

class EventConsumer(object):
    def __init__(self):
        self.cond = threading.Condition()
        self.data = None

    def consume(self, data):
        self.cond.acquire()
        self.data = data
        self.cond.notify()
        self.cond.release()

    def get_data(self):
        self.cond.acquire()
        while self.data == None:
            self.cond.wait()
        data = self.data
        self.data = None
        self.cond.release()
        return data



