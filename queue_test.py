import unittest
import traceback
from python_queue import my_queue

class TestStringMethods(unittest.TestCase):

    def test_one_worker(self):
        try:
            s = my_queue(1)
            s.start()
            s.enqueue(2)
            s.enqueue(3)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_multiple_worker(self):
        try:
            s = my_queue(3)
            s.start()
            for i in range(0,10):
                s.enqueue(i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_worker_await(self):
        try:
            s = my_queue(2)
            s.start()
            for i in range(0,10):
                s.enqueue(i, True)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

if __name__ == '__main__':
    unittest.main()
