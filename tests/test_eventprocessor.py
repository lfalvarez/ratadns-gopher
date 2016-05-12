import datetime
import random
import redis
import redis.client
import unittest
import time as Time
import json
import fakeredis
from unittest.mock import MagicMock, call

from gopher.eventprocessor import EventConsumer, ServerDataEventProcessor, EventProcessor, TopCountEventProcessor, \
    QueriesSummaryEventProcessor


class FakeStrictRedis2(fakeredis.FakeStrictRedis):
    def pubsub(self, **kwargs):
        ps = fakeredis.FakePubSub(**kwargs)
        self._pubsubs.append(ps)

        return ps

class TestEventConsumer(unittest.TestCase):
    def test1(self):
        ec = EventConsumer()
        input_value = random.random()
        ec.consume(input_value)
        output_value = ec.get_data()
        self.assertEqual(input_value, output_value)


class TestServerDataEventProcessor(unittest.TestCase):
    def setUp(self):
        self.redis_mock = MagicMock(spec=redis.StrictRedis)  # mock redis connection
        # noinspection PyTypeChecker
        self.ep = ServerDataEventProcessor(self.redis_mock, None)

    def test_correct_channels(self):
        calls = [call('QueriesPerSecond'), call('AnswersPerSecond')]
        self.redis_mock.pubsub().subscribe.assert_has_calls(calls)

    def test_process(self):
        a = random.random()
        publish, result = self.ep.process(a)
        self.assertTrue(publish)
        self.assertEqual(result, a)


class TestEventProcessor(unittest.TestCase):
    def setUp(self):
        # First we set the data that will be "produced" by the redis pubsub listener.
        self.data = ["data to consume", "data not to consume"]
        self.data_to_consume = self.data[0]
        self.data_not_to_consume = self.data[1]

        # Second we set the redis pubsub listener
        self.redis_mock = MagicMock(spec=redis.StrictRedis)  # Redis client
        self.redis_mock.pubsub().listen = \
            MagicMock(return_value=list(map(lambda d: {'data': "\"{}\"".format(d).encode()}, self.data)))

        # Third we create the process function (that will replace the process method
        # from the EventProcessor abstract class
        self.process_side_effects = list(zip([True, False], self.data))
        self.process_mock = MagicMock(side_effect=self.process_side_effects)

        # Fourth we set the consumers to be registered
        self.consumer1_mock = MagicMock(spec=EventConsumer)  # consumer that will register
        self.consumer2_mock = MagicMock(spec=EventConsumer)  # consumer that will register and then unregister

    def test_calls_to_redis_methods(self):
        """
        Test case when we call the EventProcessor subscribe method, it has to
        call the redis pubsub subscribe method.
        """
        ep = EventProcessor(self.redis_mock)
        self.redis_mock.pubsub.assert_any_call()
        ep.subscribe("topk")
        self.redis_mock.pubsub().subscribe.assert_called_once_with("topk")

    def test_process_is_called_with_all_data(self):
        """
        Test case to check that process uses all the data received from the redis.pubsub().listen
        """
        ep = EventProcessor(self.redis_mock)
        ep.register_consumer(self.consumer1_mock)
        ep.process = self.process_mock
        ep.run()
        # process was called two times, each time called with different data
        ep.process.assert_has_calls(map(lambda d: call(d), self.data))

    def test_one_consumer_registered(self):
        """
        Test case when only one consumer is registered. Only "data to consume" should
        be consumed because the process method mock returns two tuples:
        - (true, "data to consume")
        - (false, "data not to consume")
        """
        ep = EventProcessor(self.redis_mock)
        ep.register_consumer(self.consumer1_mock)
        ep.process = self.process_mock
        ep.run()

        # consume was only called exactly once AND that time was with data_to_consume
        self.consumer1_mock.consume.assert_called_once_with(self.data_to_consume)

    def test_two_consumers_registered_and_one_unregistered_later(self):
        """
        Test case when two consumer are registered. Then the run method of EventProcessor is called.
        After that, one of the consumers unregisters and the run method is called once again.
        Finally is checked that the consumer that never unregistered was called twice, and the consumer
        that unregistered was only call once.
        """
        ep = EventProcessor(self.redis_mock)
        ep.register_consumer(self.consumer1_mock)
        ep.register_consumer(self.consumer2_mock)
        ep.process = self.process_mock
        ep.run()

        self.consumer1_mock.consume.assert_called_once_with(self.data_to_consume)
        self.consumer2_mock.consume.assert_called_once_with(self.data_to_consume)

        ep.process.side_effect = self.process_side_effects
        ep.unregister_consumer(self.consumer2_mock)
        ep.run()

        # As we only unregistered consumer2_mock, consumer1_mock should have been called again
        #
        self.consumer1_mock.consume.assert_has_calls([call(self.data_to_consume)] * 2)
        self.consumer2_mock.consume.assert_called_once_with(self.data_to_consume)


class TestTopCountEventProcessor(unittest.TestCase):
    def setUp(self):
        self.r = FakeStrictRedis2()
        self.global_config_mock = {
              "redis": {
                "address": "200.7.6.140",
                "port": 6379
              },
              "servers_info": [
                {
                  "name": "beaucheff"
                },
                {
                  "name": "blanco"
                }
              ],
              "topk": {
                "times": [1, 5, 15]
              },
              "malformed": {
                "times": [1]
              }
            }
        self.class_config_mock = {"channel": "topk", "redis_set": "topk"}

    def get_time_now(self):
        return Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

    def test_increment_set(self):
        now = self.get_time_now()
        redis_set_name = "SET"
        elements_list = [({"name": "a"}, 1), ({"name": "b"}, 4), ({"name": "c"}, 2), ({"name": "d"}, 3), ({"name": "e"}, 5)]
        server_name = "blanco" # TODO: change to retrieve server from config?
        window_time = 60 # TODO: random number?
        index_time = 1 # TODO: random number?
        # TODO: test elements_list with repeated elements

        ep = TopCountEventProcessor(self.r, self.global_config_mock, self.class_config_mock)
        ep.increase_set(elements_list, [redis_set_name, "TOTAL"], now, server_name, window_time, index_time)

        results = dict(self.r.zrange(redis_set_name, 0, 4, withscores=True))
        self.assertDictEqual(results, dict([(b"a", 1), (b"b", 4), (b"c", 2), (b"d", 3), (b"e", 5)]))

        elements_list = [({"name": "f"}, 2), ({"name": "b"}, 2)]
        ep.increase_set(elements_list, [redis_set_name, "TOTAL"], now, server_name, window_time, index_time)
        results = dict(self.r.zrange(redis_set_name, 0, 5, withscores=True))
        self.assertDictEqual(results, {b"a": 1, b"b": 6, b"c": 2, b"d": 3, b"e": 5, b"f": 2})

    def test_get_old_data(self):
        now = self.get_time_now()
        redis_set_name = "SET2"
        elements_list = [({"name": "a"}, 10), ({"name": "b"}, 15), ({"name": "c"}, 5), ({"name": "d"}, 9), ({"name": "e"}, 13)]
        server_name = "blanco" # TODO: change to retrieve server from config?
        window_time = 60 # TODO: random number?
        index_time = 1 # TODO: random number?

        ep = TopCountEventProcessor(self.r, self.global_config_mock, self.class_config_mock)
        ep.increase_set(elements_list, [redis_set_name, "TOTAL"], now, server_name, window_time, index_time)
        results = ep.get_old_data(redis_set_name, 11)
        self.assertListEqual(sorted(results), sorted([b"a", b"c", b"d"]))


    def test_get_top_data(self):
        now = self.get_time_now()
        server_name = "blanco" # TODO: change to retrieve server from config?
        window_time = 14 # TODO: random number?
        redis_set_name = "topk:{}_{}".format(server_name, window_time)
        elements_list = [({"name": "a"}, 10), ({"name": "b"}, 15), ({"name": "c"}, 5), ({"name": "d"}, 9), ({"name": "e"}, 13), ({"name": "f"}, 7)]
        index_time = 1 # TODO: random number?

        ep = TopCountEventProcessor(self.r, self.global_config_mock, self.class_config_mock)
        ep.increase_set(elements_list, [redis_set_name, "TOTAL"], now, server_name, window_time, index_time)
        results = ep.get_top_data(server_name, window_time)
        self.assertDictEqual(dict(results), {b"a": 10, b"b": 15, b"d": 9, b"e": 13, b"f": 7})


    def test_format_data(self):
        ep = TopCountEventProcessor(self.r, self.global_config_mock, self.class_config_mock)
        list = [(b"a", 10), (b"b", 15), (b"d", 9), (b"e", 13), (b"f", 7)]
        result = ep.format_data(list, 54)
        self.assertListEqual(result, [("a", 10, 10/54), ("b", 15, 15/54), ("d", 9, 9/54), ("e", 13, 13/54), ("f", 7, 7/54)])


class TestQueriesSummaryEventProcessor(unittest.TestCase):
    def setUp(self):
        self.r = FakeStrictRedis2()
        self.global_config_mock = {
            "redis": {
                "address": "200.7.6.140",
                "port": 6379
            },
            "servers_info": [
                {
                    "name": "beaucheff"
                },
                {
                    "name": "blanco"
                }
            ],
            "summary": {
                "times": [1, 5, 15]
            }
        }

    def get_time_now(self):
        return Time.mktime(datetime.datetime.now().timetuple()) * 1000.0

    def test_order_data(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        element_list = [{"ip": "a", "queries": {"1": ["a11", "a12", "a13"], "2": ["a21", "a22", "a23"]}},
                        {"ip": "b", "queries": {"1": ["b11", "b12"], "2": ["b21", "b22", "b23", "b24"]}}]
        (result, total) = ep.order_data(element_list)
        self.assertEqual(result, element_list)
        self.assertEqual(total, 12)


    def test_increase_timespan(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        redis_set_name = "SET3"
        old_element_list = [("ip1", 9), ("ip2", 7), ("ip3", 13), ("ip4", 2)]
        for element in old_element_list:
            self.r.zincrby(redis_set_name, element[0], element[1])

        new_element_list = [{"ip": "ip1"}, {"ip": "ip2"}, {"ip": "ip3"}]
        ep.increase_timespan(30, [redis_set_name, "TOTAL1"], new_element_list, 0)

        result = self.r.zrange(redis_set_name, 0, 3, withscores=True)
        # self.assertDictEqual(dict(result), {b"ip1": 30, b"ip2": 30, b"ip3": 30, b"ip4": 2})

    def test_increase_set(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        server_name = "blanco" # TODO: change to retrieve server from config?
        window_time = 14 # TODO: random number?
        self.r.flushall()

        redis_set_name = "summary:historic_{}_{}".format(server_name, window_time)
        redis_total_set_name = "summary:ip_size_{}_{}".format(server_name, window_time)
        old_element_list = [{"ip": "a", "queries": {"1": ["a11", "a12", "a13"], "2": ["a21", "a22", "a23"]}},
                            {"ip": "b", "queries": {"1": ["b11", "b12"], "2": ["b21", "b22", "b23", "b24"]}}]
        for element in old_element_list:
            self.r.hset(redis_set_name, element["ip"], [(element["queries"], 5)])

        new_element = [{"ip": "c", "queries": {"f": ["c11", "c12"], "g": ["c21", "c22", "c23"]}}]
        ep.increase_set(new_element, redis_set_name, 7, server_name, window_time, 0)

        result = self.r.hgetall(redis_set_name)

        self.assertCountEqual(dict(result), {b"a": b"[({'1': ['a11', 'a12', 'a13'], '2': ['a21', 'a22', 'a23']}, 5)]",
                                            b"b": b"[({'1': ['b11', 'b12'], '2': ['b21', 'b22', 'b23', 'b24']}, 5)]",
                                            b"c": b'[[{"g": ["c21", "c22", "c23"], "f": ["c11", "c12"]}, 7]]'})

        result_total = self.r.hgetall(redis_total_set_name)
        self.assertDictEqual(result_total, {b"c": 5})

        new_element = [{"ip": "c", "queries": {"f": ["c211", "c212"], "h": ["c221", "c222", "c223"]}}]
        ep.increase_set(new_element, redis_set_name, 9, server_name, window_time, 0)
        result = self.r.hgetall(redis_set_name)

        self.assertCountEqual(dict(result), {b"a": b"[({'1': ['a11', 'a12', 'a13'], '2': ['a21', 'a22', 'a23']}, 5)]",
                                             b"b": b"[({'1': ['b11', 'b12'], '2': ['b21', 'b22', 'b23', 'b24']}, 5)]",
                                             b"c": b'[[{"f": ["c211", "c212"], "h": ["c221", "c222", "c223"]}, 9], '
                                                   b'[{"g": ["c21", "c22", "c23"], "f": ["c11", "c12"]}, 7]]'})
        result_total = self.r.hgetall(redis_total_set_name)
        #self.assertDictEqual(result_total, {b"c": 10})

    def test_cleanup_old_data(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        server_name = "blanco"  # TODO: change to retrieve server from config?
        window_time = 14  # TODO: random number?

        redis_set_name = "summary:historic_{}_{}".format(server_name, window_time)
        redis_total_set_name = "summary:ip_size_{}_{}".format(server_name, window_time)
        redis_timestamp_set = "TIMESTAMP"
        old_element_list = [{"ip": "a", "queries": {"1": ["a11", "a12", "a13"], "2": ["a21", "a22", "a23"]}},
                            {"ip": "b", "queries": {"1": ["b11", "b12"], "2": ["b21", "b22", "b23", "b24"]}}]
        ep.increase_set(old_element_list,redis_set_name, 3, server_name, window_time, 0)
        new_element = [{"ip": "c", "queries": {"f": ["c11", "c12"], "g": ["c21", "c22", "c23"]}}]
        ep.increase_set(new_element, redis_set_name, 7, server_name, window_time, 0)

        timestamp_list = [("a", 3), ("b", 3), ("c", 7)]
        for element in timestamp_list:
            self.r.zincrby(redis_timestamp_set, element[0], element[1])

        ep.cleanup_old_data([redis_total_set_name, redis_timestamp_set, redis_set_name], redis_timestamp_set, 6)

        result_total = self.r.zrange(redis_total_set_name, 0, 2)
        self.assertListEqual(result_total, [b"c"])

        result_timestamp = self.r.zrange(redis_timestamp_set, 0, 2, withscores=True)
        self.assertListEqual(result_timestamp, [(b"c", 7)])

        result_hash = self.r.hgetall(redis_set_name)
        self.assertCountEqual(result_hash, {b"c": b'[({"f": ["c11", "c12"], "g": ["c21", "c22", "c23"]}, 7)]'})

    def test_get_top_data(self):
        self.maxDiff = None
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        self.r.flushall()
        server_name = "blanco" # TODO: change to retrieve server from config?
        window_time = 14 # TODO: random number?
        redis_set_name = "summary:historic_{}_{}".format(server_name, window_time)

        old_element_list = [{"ip": "a", "queries": {"1": ["a11", "a12"], "2": ["a21", "a22", "a23"]}},
                            {"ip": "b", "queries": {"1": ["b11", "b12", "b13"]}}, {"ip": "c", "queries": {"1": ["c11"]}},
                            {"ip": "d", "queries": {"1": ["d11", "d12"]}}, {"ip": "f", "queries": {"1": ["f11", "f12"]}},
                            {"ip": "e", "queries": {"1": ["e11", "e12"]}}]
        ep.increase_set(old_element_list, redis_set_name, 7, server_name, window_time, 0)

        result = ep.get_top_data(server_name, window_time)
        self.assertCountEqual(result, [("a", {"1": ["a11", "a12"], "2": ["a21", "a22", "a23"]}, 5.0),
                                       ("b", {"1": ["b11", "b12", "b13"]}, 3.0),
                                       ("d", {"1": ["d11", "d12"]}, 2.0),
                                       ("e", {"1": ["e11", "e12"]}, 2.0),
                                       ("f", {"1": ["f11", "f12"]}, 2.0)])

    def test_format_data(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        list = [(b"b", b"{'1': ['b11', 'b12', 'b13', 'b14'], '2': ['b21', 'b22', 'b23', 'b24']}", 8),
                (b"a", b"{'1': ['a11', 'a12', 'a13'], '2': ['a21', 'a22', 'a23']}", 6),
                (b"d", b"{'1': ['c11', 'c12', 'c13'], '2': ['c21']}", 4),
                (b"e", b"{'2': ['e11', 'e12', 'e13']}", 3),
                (b"f", b"{'3': ['f11', 'f12']}", 2)]
        result = ep.format_data(list, 23)
        self.assertListEqual(result, [(b"b", b"{'1': ['b11', 'b12', 'b13', 'b14'], '2': ['b21', 'b22', 'b23', 'b24']}", 8, 8/23),
                                      (b"a", b"{'1': ['a11', 'a12', 'a13'], '2': ['a21', 'a22', 'a23']}", 6, 6/23),
                                      (b"d", b"{'1': ['c11', 'c12', 'c13'], '2': ['c21']}", 4, 4/23),
                                      (b"e", b"{'2': ['e11', 'e12', 'e13']}", 3, 3/23),
                                      (b"f", b"{'3': ['f11', 'f12']}", 2, 2/23)])

    def test_collapse_by_type(self):
        ep = QueriesSummaryEventProcessor(self.r, self.global_config_mock)
        list = [({'1': ['b11', 'b12', 'b13', 'b14'], '2': ['b21', 'b22', 'b23', 'b24']}, 10),
                ({'1': ['a11', 'a12', 'a13'], '2': ['a21', 'a22', 'a23']}, 9),
                ({'1': ['c11', 'c12', 'c13'], '2': ['c21']}, 4),
                ({'2': ['e11', 'e12', 'e13']}, 3)]
        result = ep.collapse_by_type(list)
        self.assertCountEqual(result, {'1': ['b11', 'b12', 'b13', 'b14','a11', 'a12', 'a13', 'c11', 'c12', 'c13'],
                                      '2': ['a21', 'a22', 'a23', 'b21', 'b22', 'b23', 'b24', 'c21', 'e11', 'e12', 'e13']})
