import datetime
import random
import unittest
import time as Time

from gopher.eventprocessor import \
        EventConsumer, \
        MovingWindow, \
        ServerDataEventProcessor, \
        EventProcessor, \
        TopQNamesEventProcessor, \
        QueriesSummaryEventProcessor


class TestEventConsumer(unittest.TestCase):
    def test_get_data_after_consume(self):
        ec = EventConsumer()
        input_value = random.random()
        ec.consume(input_value)
        output_value = ec.get_data()
        self.assertEqual(input_value, output_value)


# class TestEventProcessor(unittest.TestCase):
#     def setUp(self):
#         pass
# 
#     def test_register_consumer(self):
#         pass

class TestMovingWindow(unittest.TestCase):
    def setUp(self):
        self.moving_window = MovingWindow()

    def test_add_10_items_and_get(self):
        test_list = [i for i in range(0,10)]
        for i in test_list:
            self.moving_window.add_item(i, i)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

        items = self.moving_window.get_items_after_limit(4)
        self.assertListEqual(items, [i for i in range(5, 10)])

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, test_list)

    def test_add_10_items_remove_and_get(self):
        for i in range(0,10):
            self.moving_window.add_item(i, i)
        self.moving_window.remove_old_data(4)

        items = self.moving_window.get_items_after_limit(-1)
        self.assertListEqual(items, [i for i in range(5, 10)])


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
