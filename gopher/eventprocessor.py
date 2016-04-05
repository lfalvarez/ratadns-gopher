import queue
from typing import Mapping, Tuple, Any

import redis
import threading
import json

import socket
import struct
import urllib.request
import datetime
import time as Time


class EventConsumer(object):
    def __init__(self):
        self.queue = queue.Queue()

    def consume(self, data):
        self.queue.put(data)

    def get_data(self):
        print("Queue size={}".format(self.queue.qsize()))
        return self.queue.get()


class EventProcessor(threading.Thread):
    """One event processor for each URL"""

    def __init__(self, r: redis.StrictRedis):
        threading.Thread.__init__(self)
        self.pubsub = r.pubsub(ignore_subscribe_messages=True)
        self.consumers = []

    def register_consumer(self, event_consumer: EventConsumer):
        self.consumers.append(event_consumer)

    def unregister_consumer(self, event_consumer: EventConsumer):
        self.consumers.remove(event_consumer)

    def subscribe(self, channel: str):
        self.pubsub.subscribe(channel)

    def run(self):
        for serialized_item in self.pubsub.listen():
            item = json.loads(str(serialized_item['data'], "utf-8"))
            (do_consume, processed_item) = self.process(item)
            if do_consume:  # processed items must be published
                for consumer in self.consumers:
                    consumer.consume(processed_item)

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        pass


class ServerDataEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis):
        super().__init__(r)
        self.subscribe("QueriesPerSecond")
        self.subscribe("AnswersPerSecond")

    def process(self, item):
        return (True, item)


def hex_to_ip(ip_hex: str):
    if len(ip_hex) == 8 or len(ip_hex) == 7:
        ip = int(ip_hex, 16)
        return socket.inet_ntoa(struct.pack(">L", ip))
    else:  # IPV6 Not supported! (yet)
        return None  # Refactor this!


class QueriesSummaryEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe("QueriesSummary")
        self.config = config

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        for summary_entry in item['data']:
            ip = hex_to_ip(summary_entry['ip'])

            if ip == None:
                continue

            summary_entry['ip'] = ip
            url = "http://" + self.config['freegeoip']['address'] + ":" + str(self.config['freegeoip']['port']) + \
                  "/json/" + ip
            with urllib.request.urlopen(url) as freegeoip_server:
                location = json.loads(str(freegeoip_server.read(), "utf-8"))
                summary_entry['location'] = location

        return (True, item)


def order_name_data(item: Mapping[str, int]) -> list:
    ordered_data = []

    for name_counter in item:
        ordered_data.append((name_counter, item[name_counter]))

    ordered_data = sorted(ordered_data, key=get_count)
    return ordered_data


def get_count(item: Tuple[str, int])-> int:
    return item[1]


def format_sse_topk_message():
    return None


class TopKEventProcessor(EventProcessor):
    def __init__(self, r: redis.StrictRedis, config: Mapping[str, Any]):
        super().__init__(r)
        self.subscribe("topk")
        self.redis = r
        self.config = config

    def process(self, item: Mapping[str, Any]) -> Tuple[bool, Any]:
        # Paso 1-> Ordenar los valores recien llegados en orden descendente
        data = {}
        server_name = item['serverId']
        item.pop('serverId')
        name_dict = item['data']
        name_tuples = order_name_data(name_dict)

        # Paso 2-> Agregar los valores recien llegados al conjunto correspondiente
        now = Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

        for time_index in range(0, len(self.config['topk']['times'])):
                time = self.config['topk']['times'][time_index]*60
                self.redis.zadd("historic_jsons_"+server_name, now, json.dumps(name_tuples))

                # Paso 3-> Correr los scripts, que se encargan de aumentar el valor de los elementos que ya estan en el conjunto
                #         Multi existe en python y se puede usar perfectamente
                multi = self.redis.pipeline()
                for element in name_tuples:
                    #TODO:Refactor this asap!
                    multi.zincrby("topk_{}_{}".format(time, server_name), element[0], element[1])
                    multi.zincrby("global_{}".format(time), element[0], element[1])

                multi.execute()

                # Paso 4->Actualizar la ventana, sacando los valores que quedan fuera de ella. Son varios pasos, pero en general:
                #        es solamente poner bien los scripts de redis que hacen estas cosas.
                #TODO: Check this, it's probably really wrong
                script = """local old_jsons = redis.call('zrangebyscore', KEYS[1], '-inf' , ARGV[1]);
                            redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1]);
                            return old_jsons;"""

                time_range = now - time *1000
                get_json = self.redis.register_script(script)

                jsons = get_json(keys=["historic_jsons_"+server_name], args=[time_range])

                for i in range(0, len(jsons)):
                    old_queries = json.loads(jsons[i].decode("utf-8"))

                    for j in range(0, len(old_queries)):
                        multi.zincrby("topk_{}_{}".format(time, server_name), old_queries[j][0], -1*old_queries[j][1])
                        multi.zincrby("global_{}".format(time), old_queries[j][0],  -1*old_queries[j][1])

                    multi.zremrangebyscore("topk_{}_{}".format(time, server_name), "-inf", 0)
                    multi.zremrangebyscore("global_{}".format(time), "-inf", 0)

                multi.execute()

                #        1. Busco los old_jsons y si el tiempo que llevan es mayor al tiempo maximo se borran directamente
                #        2. Por cada par de en el par (json, tiempo que lleva) actualizo su valor, restando el tiempo que ha pasado
                #        3. Si algun resultado queda negativo, se elimina del topk

                global_top = self.redis.zrevrange("global_{}".format(time), 0, 4, withscores=True)
                # Paso 5->Retornar los elementos pedidos

                time_data = {}
                for server in self.config['servers']:
                    time_data[server['name']] = list(map(lambda x: (x[0].decode("utf-8"), x[1]),
                                                  self.redis.zrevrange("topk_{}_{}".format(time, server['name']), 0, 4,
                                                  withscores=True)))

                time_data['global'] = list(map(lambda x: (x[0].decode("utf-8"), x[1]), global_top))

                data[self.config['topk']['times'][time_index]] = time_data

        item['data'] = json.dumps(data)
        return (True,item)











