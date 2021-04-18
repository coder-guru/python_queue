import unittest
import traceback
import time, random,uuid
import os
import multiprocessing as mp
from multiprocessing.managers import BaseManager

import time
import thread
import sys

from python_queue import my_queue, gen_topic_queue, topic_config, gen_queue, timer_queue
from python_queue import trace_queue, message_loop, message_obj, message_handler,gc_queue
from python_queue import global_args
from python_queue import STATUS, DaemonApp,kill_topic_handler
import python_queue

class q1_topic_handler(gen_queue):
    def do_work_handler(self):
        return q1_topic_handler._do_work

    @classmethod
    def _do_work(cls,worker,item):
        time.sleep(random.randint(2,5))
        v = item.item['msg']
        #for testing error message
        if v == '9999':
            raise Exception('Force Fail!')
        print("q1_topic_handler - {0}".format(v))

class q2_topic_handler(gen_queue):
    def do_work_handler(self):
        return q2_topic_handler._do_work

    @classmethod
    def _do_work(cls,worker,item):
        print('Here he')
        v = item.item['msg']
        print("q2_topic_handler - {0}".format(v))

class calc_multiplier(gen_queue):
    def do_work_handler(self):
        return calc_multiplier._do_work

    @classmethod
    def _do_work(cls,worker,item):
        v = item.item
        print("Out - {0}".format(v))
        v = v ** (v % 4)

def my_handler():
    return handle

def handle(msg):
        for i in range(msg * 5,(msg + 1) * 5):
            print('loop {0}'.format(i))
            yield

class TestStringMethods(unittest.TestCase):

    def test_one_worker(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,1)
            s.start()
            s.enqueue_async(2)
            s.enqueue_async(3)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_multiple_worker(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,3)
            s.start()
            for i in range(0,10):
                s.enqueue_async(i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_worker_await(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,2)
            s.start()
            for i in range(0,5):
                wait = s.enqueue_async(i)
                status,_ = s.enqueue_await(wait)
                self.failIf(status is not STATUS.SUCCESS())
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue(self):
        g_args = global_args()    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', my_queue, 1))
            s = gen_topic_queue(g_args,q_config, 1)
            s.start()
            for i in range(0,5):
                s.enqueue_async({'topic':'.q1.number','num':i,})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi(self):
        g_args = global_args()    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 2))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 2))
            s = gen_topic_queue(g_args, q_config, 2)
            s.start()
            for i in range(0,10):
                if i % 2 == 0:
                    wait = s.enqueue_async({'topic':'.q1.number','msg':i,})
                    status,_ = s.enqueue_await(wait)
                    self.failIf(status is not STATUS.SUCCESS())
                else:
                    s.enqueue_async({'topic':'.q2.number','msg':i,})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi_large(self):
        g_args = global_args()    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 3))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 3))
            s = gen_topic_queue(g_args,q_config, 6)
            s.start()
            for i in range(0,10):
                if i % 2 == 0:
                    s.enqueue_async({'topic':'.q1.number','msg':i,})
                else:
                    s.enqueue_async({'topic':'.q2.number','msg':i,})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_timer_queue(self):
        g_args = global_args()    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 1))
            s = gen_topic_queue(g_args,q_config, 1)
            s.start()
            t = timer_queue(g_args,5,{'topic':'.q1.timer','msg':1,},s)
            t.start()
            time.sleep(20)
            t.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_queue_tracing(self):
        g_args = global_args()    
        try:
            s = trace_queue(g_args,3)
            t = my_queue(g_args,2)
            s.start()
            t.set_trace(s)
            t.start()
            for i in range(0,10):
                t.enqueue_async(i)
            t.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_message_loop(self):
        g_args = global_args()    
        try:
            s = message_loop(g_args)
            s1 = message_loop(g_args)
            s.start()
            s1.start()
            for i in range(0,5):
                s.enqueue_async(message_obj(i,my_handler()))
            for i in range(5,10):
                s1.enqueue_async(message_obj(i,my_handler()))            
            print('enqueue_async Done!')
            s1.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())


    def test_kill(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,1)
            s.start()
            for i in range(0,5):
                wait = s.enqueue_async(i)
                if i == 3:
                    pass
                    self.failIf(s.kill(wait) == False)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_kill_all_cases(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,1)
            s.start()
            #kill in not existing
            self.failIf(s.kill('asdfgf;lkjhj') == True)
            w1 = s.enqueue_async(1)
            w2 = s.enqueue_async(1)
            time.sleep(2)
            self.failIf(s.kill(w1) == False)
            self.failIf(s.kill(w2) == False)
            s.stop()

        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())        

    def test_error_msg(self):
        g_args = global_args()    
        try:
            s = q1_topic_handler(g_args,1)
            s.start()
            wait = s.enqueue_async({'topic':'.q1.timer','msg':'9999',})
            result, msg = s.enqueue_await(wait)
            self.failIf(result is not STATUS.FAIL())
            print(msg)
            wait = s.enqueue_async({'topic':'.q1.timer','msg':'888',})
            result,_ = s.enqueue_await(wait)
            self.failIf(result is not STATUS.SUCCESS())
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())
    def test_load1(self):
        g_args = global_args()    
        try:
            s = calc_multiplier(g_args,100)
            s.start()
            for i in range(0,100):
                s.enqueue_async(i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_server2(self):
        app = DaemonApp("localhost",8000)
        python_queue._app = app
        try:
            try:
                g_args = global_args()    
                q_config = []
                q_config.append(topic_config('.q1.normal.*', q1_topic_handler, 1))
                q_config.append(topic_config('.q1.kill', kill_topic_handler, 1))
                tq = gen_topic_queue(g_args,q_config, 1)
                tq.start()
                tq.enqueue_async({'topic':'.q1.normal','msg':'Msg1.',})
                tq.enqueue_async({'topic':'.q1.normal','msg':'Msg2.',})
                tq.enqueue_async({'topic':'.q1.normal','msg':'Msg3.',})
                tq.enqueue_async({'topic':'.q1.kill','msg':'Kill!',})
                app.start_app()
            except Exception as ex:
                print(ex)
            finally:
                tq.stop()
        except KeyboardInterrupt:
            pass
        try:
            app.stop_app()
        finally:
            pass

    def test_gc_collect(self):
        g_args = global_args()    
        try:
            s = my_queue(g_args,1)
            g = gc_queue(g_args)
            t = timer_queue(g_args,5,{'topic':'.q4.gc','msg':1,},g)
            g.start()
            s.start()
            t.start()
            w = s.enqueue_async(2)
            s.enqueue_async(3)
            time.sleep(10)
            s.enqueue_async(5)
            time.sleep(30)
            t.stop()
            s.stop()
            g.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())


if __name__ == '__main__':
    unittest.main()
