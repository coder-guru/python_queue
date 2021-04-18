"""  
TODO: Support Kill
TODO: Support sending message to main publish queue
TODO: cleanup status dictionary of expired items
"""
import traceback, sys, os
import multiprocessing
from multiprocessing import Manager
import Queue
import time, datetime
import re
import random
import uuid
from ctypes import c_char_p

import SimpleHTTPServer
import SocketServer
import thread
import socket
import sys

_app = None

_manager = Manager()
#keep all instance of queue (for broadcast type of processing)
_all_q = _manager.dict()

class STATUS:
    @classmethod
    def WAIT(cls): return 0
    @classmethod
    def START(cls): return 1
    @classmethod
    def SUCCESS(cls): return 2
    @classmethod
    def FAIL(cls): return 3
    @classmethod
    def KILL(cls): return 4

TRACE_LEVEL = 0

class global_args(object):
    def __init__(self):
        self.work_status = _manager.dict()
        self.error_msg = _manager.dict()
        
class handler_args(object):
    def __init__(self,target_q,global_args,pre_work,do_work,post_work,all_done,forwarder):
        self.target_q = target_q
        self.global_args = global_args
        self.pre_work = pre_work
        self.do_work = do_work
        self.post_work = post_work
        self.all_done = all_done
        self.is_forwarder = forwarder

        #signal to quit processing
        self.all_done_signal = _manager.Value('b', 0)

class Worker(multiprocessing.Process):
    def __init__(self, worker_id, handler, args):
        gen_queue.trace_msg("Init Worker...")
        self.__now_processing = _manager.Value(c_char_p, None)
        super(Worker, self).__init__(target=handler,name=worker_id,args=(args,))
        gen_queue.trace_msg("Init Worker...Done.")

    def now_processing(self):
        return self.__now_processing.value

    def set_now_processing(self,message_id):
        self.__now_processing.value = message_id

class MessageKilled(Exception):
    pass
class queue_item(object):
    def __init__(self, item,id=None):
        self.item = item
        if id is None:
            self.id = uuid.uuid4()
        else:
            self.id = id      
class gen_queue(object):
    def __init__(self,g_args, num_workers):
        self.g_args = g_args
        self.q = multiprocessing.Queue()
        self.args = handler_args(
                    self.q,
                    g_args,
                    self.pre_work_handler(),
                    self.do_work_handler(),
                    self.post_work_handler(),
                    gen_queue.all_done,
                    False
                    )
        if num_workers <= 0:
            num_workers = 1
        self.num_workers = num_workers
        self.workers = {}
        self.trace_q = None
        self.id = uuid.uuid4()

    def pre_work_handler(self):
        return gen_queue._pre_work

    @classmethod
    def _pre_work(cls,worker,args,item):
        gen_queue.trace_msg("Pre work...")
        try:
            if not args.is_forwarder:
                #if kill request is already made, do not process
                gen_queue.trace_msg("status -> {0}".format(args.global_args.work_status[item.id]))
                if args.global_args.work_status[item.id] is not STATUS.KILL():
                    gen_queue.trace_msg("Set Now processing - ")
                    worker.set_now_processing(item.id)
                    args.global_args.work_status[item.id] = STATUS.START()
                else:
                    #raise exception
                    raise MessageKilled('Message killed while waiting!')
                    
        except KeyError as ex:
            pass
        finally:
            pass

    def do_work_handler(self):
        raise NotImplementedError('Derived class to implement this method.')

    @classmethod
    def _do_work(cls,worker,item):
        gen_queue.trace_msg("Do work...")
        raise NotImplementedError('_do_work Not Implemented.')

    @classmethod
    def all_done(cls,worker,args):
        return args.all_done_signal.value == 1

    def post_work_handler(self):
        return gen_queue._post_work

    @classmethod
    def _post_work(cls,worker,args,item,status, error_msg):
        gen_queue.trace_msg("Post work...")
        try:
            #if not a forwarder, then set status
            if not args.is_forwarder:
                if args.global_args.work_status[item.id] is not STATUS.KILL():
                    args.global_args.work_status[item.id] = status
                    args.global_args.error_msg[item.id] = error_msg
                worker.set_now_processing(None)

        except KeyError as ex:
            pass
        finally:
            pass

    @classmethod
    def handle_work(cls,args):
        worker = multiprocessing.current_process()
        gen_queue.trace_msg('Process Name - {0}'.format(worker.name))
        status = None
        err_msg = None
        while True:
            try:
                gen_queue.trace_msg('about to get queue')
                status = STATUS.START()
                obj = None
                obj = args.target_q.get(True,5)
                gen_queue.trace_msg('done get queue')
                try:
                    if args.pre_work is not None:
                        args.pre_work(worker,args,obj)
                    gen_queue.trace_msg('about to process')
                    if args.do_work is not None:
                        args.do_work(worker,obj)
                    gen_queue.trace_msg('done process')
                    status = STATUS.SUCCESS()
                except MessageKilled as ex1:
                    status = STATUS.KILL()
                    err_msg = str(ex1)
                    gen_queue.trace_msg(err_msg, 4)
                    pass
                except Exception as ex:
                    gen_queue.trace_msg('Inner Error Processing...', 4)
                    gen_queue.trace_msg(ex,4)
                    #traceback.print_stack()
                    #set error attributes
                    status = STATUS.FAIL()
                    err_msg = str(ex)

                finally:
                    if args.post_work is not None:
                        args.post_work(worker,args,obj, status,err_msg)
                    pass

            except Queue.Empty:
                gen_queue.trace_msg('Queue Empty...')
                if args.all_done(worker, args):
                    gen_queue.trace_msg('Breaking out...')
                    break 
            except Exception as ex:
                gen_queue.trace_msg('Error Processing...', 4)
                gen_queue.trace_msg(ex,4)
                traceback.print_stack()
                break
            finally:
                pass

        gen_queue.trace_msg('All Done!', 1)

    def start(self):
        for i in range(0,self.num_workers):      
            self.add_worker()
        #_all_q[self.id] = self.q

    def add_worker(self):
        worker_id = uuid.uuid4()
        w = Worker(worker_id,gen_queue.handle_work,self.args)
        self.workers[worker_id] = w
        w.start()

    def remove_worker(self, worker_id):
        try:
            worker = self.workers[worker_id]
            self.workers.pop(worker_id)
            worker.terminate()
        except Exception as ex:
            gen_queue.trace_msg('remove_worker() failed.',4)
            gen_queue.trace_msg(ex, 4)
        finally:
            pass

    def set_trace(self, trace_q):
        self.trace_q = trace_q

    @classmethod
    def who_am_i(cls, title='who_am_i()'):
        # print(title)
        # print('module name:', __name__)
        # print('parent process:', os.getppid())
        # print('process id:', os.getpid())
        pass

    def _enqueue(self, obj):
        gen_queue.who_am_i('Enqueue()')
        #create queue item and then enque
        #check for nested queue items
        queue_chaining = isinstance(obj, queue_item)
        if queue_chaining:
            item = obj
        else:
            item = queue_item(obj)
        # check if tracing is set
        if self.trace_q is not None:
            self.trace_q.enqueue_async(item)

        self.g_args.work_status[item.id] = STATUS.WAIT()
        self.args.target_q.put(item, False)
        return item.id

    def enqueue_async(self, obj):
        return self._enqueue(obj)

    def enqueue_await(self, async_id):
        status = None
        error_msg = None
        while True:
            try:
                status = self.g_args.work_status[async_id]
                if status == STATUS.START() or status == STATUS.WAIT():
                    time.sleep(5)
                else:
                    error_msg = self.g_args.error_msg[async_id]
                    break
            except KeyError:
                break
            except Exception as ex:
                gen_queue.trace_msg('enqueue_await() Exception.', 4)
                gen_queue.trace_msg(ex,4)
            finally:
                pass

        #remove async_id from list
        try:
            self.g_args.work_status.remove(async_id)
            self.g_args.error_msg.remove(async_id)
        except:
            pass
        finally:
            pass
        return (status, error_msg)

    def stop(self):
        #gen_queue._all_queue.remove(self)
        self.args.all_done_signal.value = 1
        for key, worker in self.workers.items():
            worker.join()

    def kill(self, async_id):
        gen_queue.trace_msg('Kill recieved id {0}'.format(async_id))
        killed = False
        try:
            #find the status of the request
            status = self.g_args.work_status[async_id]
            print('status -', status)
            if status == STATUS.START():
                #find the worker that is processing the message
                for worker_id, w in self.workers.items():
                    print('w - ',w.now_processing())
                    if w.now_processing() == async_id:
                        gen_queue.trace_msg('Killing worker!')
                        self.remove_worker(worker_id)
                        gen_queue.trace_msg('Kill was successful!')
                        killed = True
                        break
            elif status == STATUS.WAIT():
                #set the status to kill
                self.g_args.work_status[async_id] = STATUS.KILL()
                gen_queue.trace_msg('Kill request made!')
                killed = True
        except KeyError:
            pass
        except Exception as ex:
            gen_queue.trace_msg('Kill process error!', 4)
            gen_queue.trace_msg(ex, 4)
        finally:
            pass
        if killed:
            self.add_worker()
        return killed

    @classmethod
    def trace_msg(cls,msg, level=0):
        if level >= TRACE_LEVEL:
            print(msg)
        pass

    @classmethod
    def all_queue_kill(cls,key):
        for q in gen_queue._all_queue:
            try:
                q.kill(key)
                pass
            except Exception as ex:
                gen_queue.trace_msg(ex, 4)
            finally:
                pass
        pass
class my_queue(gen_queue):
    def do_work_handler(self):
        return my_queue._do_work

    @classmethod
    def _do_work(cls,worker,item):
        time.sleep(5)
        gen_queue.trace_msg("my_queue {0}".format(item.item))

class topic_config(object):
    def __init__(self, topic, handler_cls, num_workers):
        self.topic = topic
        self.handler_cls = handler_cls
        self.num_workers = num_workers

class gen_topic_queue(gen_queue):
    def __init__(self, g_args, topic_config_arr, num_workers=1):
        super(gen_topic_queue, self).__init__(g_args,num_workers)
        #set forwarder
        self.args.is_forwarder = True
        self.__topic_q = []
        for topic_config in topic_config_arr:
            #create queues to handle each topic
            q = topic_config.handler_cls(g_args, topic_config.num_workers)
            self.__topic_q.append({topic_config.topic : q})

    def start(self):
        super(gen_topic_queue, self).start()
        for i in range(0,len(self.__topic_q)):
            self.__topic_q[i].values()[0].start()

    def stop(self):
        for i in range(0,len(self.__topic_q)):
            self.__topic_q[i].values()[0].stop()
        super(gen_topic_queue, self).stop()

    def do_work_handler(self):
        return self._do_work

    def _do_work(self,worker,item):
        #match topic and send to associated queue
        orphen_message = True
        for i in range(0,len(self.__topic_q)):
            p = re.compile(self.__topic_q[i].keys()[0].replace(".","[.]"))
            if p.match(item.item['topic']) is not None:
                #queue item to repective queue. honor await request
                self.__topic_q[i].values()[0].enqueue_async(item)
                orphen_message = False
                break
        if orphen_message:
            gen_queue.trace_msg("Unhandled topic: {0}".format(obj.item['topic']))
class timer_queue(gen_queue):
    def __init__(self, g_args,num_seconds, msg, q):
        self.num_seconds = num_seconds
        self.msg = msg
        self.target_q = q
        super(timer_queue, self).__init__(g_args,1)

    def start(self):
        super(timer_queue, self).start()
        self.enqueue_async(1)

    def do_work_handler(self):
        return self._do_work

    def _do_work(self,worker,item):
        gen_queue.trace_msg('Timer msg delivery...')
        self.target_q.enqueue_async(self.msg)
        time.sleep(self.num_seconds)
        #check if we are done.
        if not self.args.all_done_signal.value == 1:        
            self.enqueue_async(1)

class trace_queue(gen_queue):
    def do_work_handler(self):
        return trace_queue._do_work

    @classmethod
    def _do_work(cls,worker,item):
        gen_queue.trace_msg('{2} id:{0} msg:{1}'.format(item.id,item.item,datetime.datetime.now()))

class message_obj(object):
    def __init__(self, item, fn):
        self.item = item
        self.fn = fn

#message handler base class
class message_handler(object):
    def handle(self, msg):
        raise NotImplementedError

class message_loop(gen_queue):
    def __init__(self, g_args):
        self.execute_list = []
        super(message_loop, self).__init__(g_args,1)

    def do_work_handler(self):
        return message_loop._get_processor(self)

    @classmethod
    def _get_processor(cls, queue_obj):
        #closure
        execute_list = []

        def _msg_process(worker, obj):
            #process all the messages
            #add to list except, loop back message            
            if obj is None:
                gen_queue.trace_msg('object is none')
            if obj.item.fn is not None:
                try:
                    execute_list.append(obj.item.fn(obj.item.item))
                    pass
                except Exception as ex:
                    gen_queue.trace_msg('Error Messsage_loop __Process()', 4)
                    gen_queue.trace_msg(ex, 4)
                finally:
                    pass

            # process all
            for generator in execute_list:
                if obj is not None:
                    try:
                        next(generator)
                    except StopIteration:
                        execute_list.remove(generator)
                    finally:
                        pass
            # add loop back message, if there is pending messages to process
            if len(execute_list) > 0:
                gen_queue.trace_msg('loop enqueue')
                queue_obj.enqueue_async(message_obj(None,None))

        return _msg_process

class kill_topic_handler(gen_queue):
    def do_work_handler(self):
        return kill_topic_handler._do_work

    @classmethod
    def _do_work(cls,worker,item):
        v = item.item['msg']
        post_data = 'POST /{0} HTTP/1.0 \n\
From: localhost\n\
User-Agent: internal\n\
Content-Type: application/x-www-form-urlencoded\n\
Content-Length: 8\n\
\n\
msg=kill\n\
'
    
        #for testing error message
        if v == 'Kill!':
            print("Kill received - {0}".format(v))
            kill_topic_handler._kill_app(_app, post_data.format(_app.get_id()))
        print("kill_topic_handler - {0}".format(v))

    @classmethod
    def _kill_app(cls,app,data):

        server_address = app.get_server_address()

        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Connect to server and send data
            sock.connect(server_address)
            sock.sendall(bytes(data + "\n"))
            received = str(sock.recv(1024))
        finally:
            #wait for the pipe to be clear
            time.sleep(5)
            sock.close()

class HttpHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith('/' + str(_app.get_id())):
            print "Server is going down, run it again manually!"
            def kill_me_please(server):
                server.shutdown()
            thread.start_new_thread(kill_me_please, (_app.get_server(),))
            self.send_error(500)

class MyTCPServer(SocketServer.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
    
class DaemonApp(object):
    def __init__(self, host='localhost', port=8000):
        self._server_address = (host, port)
        self._httpd = MyTCPServer(self._server_address, HttpHandler)
        self._id = str(uuid.uuid4())
        
    def get_server(self): return self._httpd

    def get_id(self): return self._id
    def get_server_address(self): return self._server_address

    def start_app(self):
        print('Starting Daemon.... id:{0}'.format(self.get_id()))
        self.get_server().serve_forever()
        pass

    def stop_app(self):
        pass

    pass

if __name__ == '__main__':
    print('Main called...')
    g_args = global_args()
    try:
        q = gen_queue(g_args,1)
        q.start()
        q.enqueue_async(1)
        q.stop()
    except Exception as ex:
        print(ex)
    finally:
        pass
