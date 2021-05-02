import unittest
import traceback
import time, random,uuid
import os
import multiprocessing as mp
from multiprocessing.managers import BaseManager
from multiprocessing import Manager
from app_dist_objects import handle_msg

import time
import sys

from dist_python_queue import my_queue, gen_topic_queue, topic_config, gen_queue, timer_queue
from dist_python_queue import trace_queue, message_loop, message_obj,gc_queue
from dist_python_queue import global_args
from dist_python_queue import DaemonApp,kill_topic_handler,shared_manager
import dist_python_queue
from dist_python_queue import ST_SUCCESS, ST_FAIL

class q1_topic_handler(gen_queue):
    def do_work_handler(self):
        return q1_topic_handler._do_work

    @classmethod
    def _do_work(cls,worker,args,item):
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
    def _do_work(cls,worker,args,item):
        print('Here he')
        v = item.item['msg']
        print("q2_topic_handler - {0}".format(v))

class calc_multiplier(gen_queue):
    def do_work_handler(self):
        return calc_multiplier._do_work

    @classmethod
    def _do_work(cls,worker,args,item):
        v = item.item
        print("Out - {0}".format(v))
        v = v ** (v % 4)

def my_handler():
    return handle_msg

class TestStringMethods(unittest.TestCase):
    _distributed = True
    
    def test_one_worker(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_one_worker',g_args,1)
            s.start()
            s.enqueue_async(2)
            s.enqueue_async(3)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_multiple_worker(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_multiple_worker',g_args,3)
            s.start()
            for i in range(0,10):
                s.enqueue_async(i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_worker_await(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_worker_await',g_args,2)
            s.start()
            for i in range(0,5):
                wait = s.enqueue_async(i)
                status,_ = s.enqueue_await(wait)
                print('wait done!')
                self.assertTrue(status is ST_SUCCESS())
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', my_queue, 1))
            s = gen_topic_queue('test_topic_queue',g_args,q_config, 1)
            s.start()
            for i in range(0,5):
                s.enqueue_async({'topic':'.q1.number','num':i,})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 2))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 2))
            s = gen_topic_queue('test_topic_queue_multi',g_args, q_config, 2)
            s.start()
            for i in range(0,10):
                if i % 2 == 0:
                    wait = s.enqueue_async({'topic':'.q1.number','msg':i,})
                    #status,_ = s.enqueue_await(wait)
                    #self.failIf(status is not python_queue.ST_SUCCESS())
                else:
                    s.enqueue_async({'topic':'.q2.number','msg':i,})
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_topic_queue_multi_large(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 3))
            q_config.append(topic_config('.q2.*', q2_topic_handler, 3))
            s = gen_topic_queue('test_topic_queue_multi_large',g_args,q_config, 6)
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
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            q_config = []
            q_config.append(topic_config('.q1.*', q1_topic_handler, 1))
            s = gen_topic_queue('test_timer_queue',g_args,q_config, 1)
            s.start()
            t = timer_queue('timer_test_timer_queue',g_args,5,{'topic':'.q1.timer','msg':1,},s)
            t.start()
            time.sleep(20)
            t.stop()
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_queue_tracing(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = trace_queue('trace_test_queue_tracing',g_args,3)
            t = my_queue('test_queue_tracing',g_args,2)
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
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = message_loop('test_message_loop',g_args)
            s1 = message_loop('test_message_loop2',g_args)
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
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_kill',g_args,1)
            s.start()
            for i in range(0,5):
                wait = s.enqueue_async(i)
                if i == 3:
                    pass
                    self.assertTrue(s.kill(wait) == True)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_kill_all_cases(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_kill_all_cases',g_args,1)
            s.start()
            #kill in not existing
            self.assertTrue(s.kill('asdfgf;lkjhj') == False)
            w1 = s.enqueue_async(1)
            w2 = s.enqueue_async(1)
            time.sleep(2)
            self.assertTrue(s.kill(w1) == True)
            self.assertTrue(s.kill(w2) == True)
            s.stop()

        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())        

    def test_error_msg(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = q1_topic_handler('test_error_msg',g_args,1)
            s.start()
            wait = s.enqueue_async({'topic':'.q1.timer','msg':'9999',})
            result, msg = s.enqueue_await(wait)
            self.assertTrue(result == ST_FAIL())
            print(msg)
            wait = s.enqueue_async({'topic':'.q1.timer','msg':'888',})
            result,_ = s.enqueue_await(wait)
            self.assertTrue(result == ST_SUCCESS())
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_load1(self):
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = calc_multiplier('test_load1',g_args,100)
            s.start()
            for i in range(0,10000):
                s.enqueue_async(i)
            s.stop()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_server2(self):
        shared_manager.set_mode(False)
        g_args = global_args(shared_manager)
        try:    
            q_config = []
            q_config.append(topic_config('.q1.normal.*', q1_topic_handler, 1))
            q_config.append(topic_config('.q1.kill', kill_topic_handler, 1))
            tq = gen_topic_queue('test_server2',g_args,q_config, 1)
            app = DaemonApp(g_args,"localhost",8000, tq)
            try:
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
        shared_manager.set_distributed(TestStringMethods._distributed)
        g_args = global_args(shared_manager)    
        try:
            s = my_queue('test_gc_collect',g_args,1)
            g = gc_queue('gc_test_gc_collect',g_args)
            t = timer_queue('timer_test_gc_collect',g_args,5,{'topic':'.q4.gc','msg':1,},g)
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

def suite(distributed):
    TestStringMethods._distributed = distributed
    suite = unittest.TestSuite()
    suite.addTest(TestStringMethods('test_one_worker'))
    suite.addTest(TestStringMethods('test_multiple_worker'))
    suite.addTest(TestStringMethods('test_worker_await'))
    suite.addTest(TestStringMethods('test_topic_queue'))
    suite.addTest(TestStringMethods('test_topic_queue_multi'))
    suite.addTest(TestStringMethods('test_topic_queue_multi_large'))
    suite.addTest(TestStringMethods('test_timer_queue'))
    suite.addTest(TestStringMethods('test_message_loop'))
    suite.addTest(TestStringMethods('test_kill'))
    suite.addTest(TestStringMethods('test_kill_all_cases'))
    suite.addTest(TestStringMethods('test_error_msg'))
    suite.addTest(TestStringMethods('test_load1'))
    suite.addTest(TestStringMethods('test_gc_collect'))
    return suite


if __name__ == '__main__':
    #unittest.main()
    runner = unittest.TextTestRunner()
    runner.run(suite(False))
    runner.run(suite(True))

