import random
import redis
import redis.client
import unittest
from unittest.mock import MagicMock, call

from gopher.eventprocessor import EventConsumer, ServerDataEventProcessor, EventProcessor


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
        self.ep = ServerDataEventProcessor(self.redis_mock)

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
        self.redis_mock = MagicMock(spec=redis.StrictRedis)
        self.consumer1_mock = MagicMock(spec=EventConsumer) # consumer that will register
        self.consumer2_mock = MagicMock(spec=EventConsumer) # consumer that will register and then unregister
        self.data_to_consume = "data to consume"
        self.data_not_to_consume = "data not to consume"
        self.process_side_effects = [(True, self.data_to_consume),
                                     (False, self.data_not_to_consume)]
        self.process_mock = MagicMock(side_effect = self.process_side_effects)
        self.redis_mock.pubsub().listen = \
            MagicMock(return_value=[{'data': "\"{}\"".format(self.data_to_consume).encode()},
                                    {'data': "\"{}\"".format(self.data_not_to_consume).encode()}])

    def test_calls_to_redis_methods(self):
        ep = EventProcessor(self.redis_mock)
        self.redis_mock.pubsub.assert_any_call()
        ep.subscribe("topk")
        self.redis_mock.pubsub().subscribe.assert_called_once_with("topk")

    def test_process_is_called_with_all_data(self):
        ep = EventProcessor(self.redis_mock)
        ep.register_consumer(self.consumer1_mock)
        ep.process = self.process_mock
        ep.run()
        # process was called two times, each time called with different data
        ep.process.assert_has_calls([call(self.data_to_consume), call(self.data_not_to_consume)])


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
        # process was called two times, each time called with different data
        ep.process.assert_has_calls([call(self.data_to_consume), call(self.data_not_to_consume)])
        # consume was only called exactly once AND that time was with data_to_consume
        self.consumer1_mock.consume.assert_called_once_with(self.data_to_consume)

    def test_two_consumers_registered_and_one_unregistered_later(self):
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

        self.consumer1_mock.consume.assert_has_calls([call(self.data_to_consume),
                                                     call(self.data_to_consume)])
        self.consumer2_mock.consume.assert_called_once_with(self.data_to_consume)
