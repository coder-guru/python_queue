import unittest
import traceback
import time, random
import os

from python_queue import my_queue, gen_topic_queue, topic_config, gen_queue, timer_queue

class q1_topic_handler(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        time.sleep(random.randint(1,5))
        v = obj.item['msg']
        print("q1_topic_handler - {0}".format(v))
class q2_topic_handler(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        v = obj.item['msg']
        print("q2_topic_handler - {0}".format(v))
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

    def test_topic_queue(self):
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', my_queue, 1))
            s = gen_topic_queue(q_config, 1)
            s.start()
            for i in range(0,5):
                s.enqueue({'topic':'.q1.number','num':i,}, False)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi(self):
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 2))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 2))
            s = gen_topic_queue(q_config, 2)
            s.start()
            for i in range(0,10):
                if i % 2 == 0:
                    s.enqueue({'topic':'.q1.number','msg':i,}, True)
                else:
                    s.enqueue({'topic':'.q2.number','msg':i,}, False)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi_large(self):
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 3))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 3))
            s = gen_topic_queue(q_config, 6)
            s.start()
            for i in range(0,10):
                if i % 2 == 0:
                    s.enqueue({'topic':'.q1.number','msg':i,}, False)
                else:
                    s.enqueue({'topic':'.q2.number','msg':i,}, False)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_timer_queue(self):
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 1))
            s = gen_topic_queue(q_config, 1)
            s.start()
            t = timer_queue(5,{'topic':'.q1.timer','msg':1,},s)
            t.start()
            time.sleep(20)
            t.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

if __name__ == '__main__':
    unittest.main()
