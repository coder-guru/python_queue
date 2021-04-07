import unittest
import traceback
import time, random
import os

from python_queue import my_queue, gen_topic_queue, topic_config, gen_queue, timer_queue
from python_queue import trace_queue, message_loop, message_obj, message_handler

class q1_topic_handler(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        time.sleep(random.randint(2,5))
        v = obj.item['msg']
        #for testing error message
        if v == '9999':
            raise Exception('Force Fail!')
        print("q1_topic_handler - {0}".format(v))

class q2_topic_handler(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        v = obj.item['msg']
        print("q2_topic_handler - {0}".format(v))
        print(len(gen_queue._all_queue))

class calc_multiplier(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        v = obj.item
        print("Out - {0}".format(v))
        v = v ** (v % 4)

class my_handler(message_handler):
    def handle(self,msg):
        for i in range(0,5):
            print('loop {0}'.format(i))
            yield

class TestStringMethods(unittest.TestCase):

    def test_one_worker(self):
        try:
            s = my_queue(1)
            s.start()
            s.enqueue(None,2,2)
            s.enqueue(None,3,3)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_multiple_worker(self):
        try:
            s = my_queue(3)
            s.start()
            for i in range(0,10):
                s.enqueue(i,i,i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_worker_await(self):
        try:
            s = my_queue(2)
            s.start()
            for i in range(0,10):
                s.enqueue(i,i, True)
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
                s.enqueue(i, {'topic':'.q1.number','num':i,}, False)
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
                    s.enqueue(i,{'topic':'.q1.number','msg':i,}, True)
                else:
                    s.enqueue(i,{'topic':'.q2.number','msg':i,}, False)
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
                    s.enqueue(i,{'topic':'.q1.number','msg':i,}, False)
                else:
                    s.enqueue(i,{'topic':'.q2.number','msg':i,}, False)
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

    def test_queue_tracing(self):
        try:
            s = trace_queue(3)
            t = my_queue(2)
            t.set_trace
            s.start()
            t.set_trace(s)
            t.start()
            for i in range(0,10):
                t.enqueue(i, i, False)
            t.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_message_loop(self):
        try:
            s = message_loop()
            s.start()
            for i in range(0,5):
                s.enqueue(i,message_obj(None,my_handler()))
            print('Enqueue Done!')
            time.sleep(10)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_kill(self):
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 1))
            s = gen_topic_queue(q_config, 1)
            s.start()
            s.enqueue(999,{'topic':'.q1.timer','msg':'Time To Kill',})
            time.sleep(2)
            gen_queue.all_queue_kill(999)
            s.enqueue(999,{'topic':'.q1.timer','msg':'Time To Be Born',})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_error_msg(self):
        try:
            s = q1_topic_handler(1)
            s.start()
            status = s.enqueue(9999, {'topic':'.q1.timer','msg':'9999',}, True)
            self.failIf(status == 0)
            status = s.enqueue(888, {'topic':'.q1.timer','msg':'888',}, True)
            self.failIf(status == 1)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_load1(self):
        try:
            s = calc_multiplier(100)
            s.start()
            for i in range(0,100000):
                s.enqueue(i,i,False)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

if __name__ == '__main__':
    unittest.main()
