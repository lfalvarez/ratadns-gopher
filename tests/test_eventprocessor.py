import datetime
import random
import redis
import redis.client
import unittest
import time as Time

from gopher.eventprocessor import EventConsumer, ServerDataEventProcessor, EventProcessor, TopQNamesEventProcessor, \
    QueriesSummaryEventProcessor


# class TestEventConsumer(unittest.TestCase):
#     def test1(self):
#         ec = EventConsumer()
#         input_value = random.random()
#         ec.consume(input_value)
#         output_value = ec.get_data()
#         self.assertEqual(input_value, output_value)
#
#
# class TestEventProcessor(unittest.TestCase):
#
#
# class TestMovingWindow(unittest.TestCase):
#
#
# class TestWindowedEventProcessor(unittest.TestCase):
#
#
# class TestQueriesSummaryEventProcessor(unittest.TestCase):
#
#
# class TestServerDataEventProcessor(unittest.TestCase):
#
#
# class TestTopQNamesEventProcessor(unittest.TestCase):
