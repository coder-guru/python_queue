"""  
TODO: Support sending message to main publish queue
TODO: cleanup status dictionary of expired items
TODO: remove all do_work that are object method.  convert to class method
"""
import traceback, sys, os
import multiprocessing
from multiprocessing import Manager
import queue
import time, datetime
import re
import random
import uuid

import http.server
import socketserver
import threading
import socket
import sys

from app_dist_objects import queue_item
from app_shared_object_client import AppSharedObjectClient

ST_WAIT = lambda : 0
ST_START = lambda : 1
ST_SUCCESS = lambda : 2
ST_FAIL = lambda : 3
ST_KILL = lambda : 4

TRACE_LEVEL = lambda : 2

_distributed_server_address = ('localhost', 50000)

class global_args(object):
    def __init__(self,shared_handler):
        self.work_status = shared_handler.get_dict('work_status')
        self.error_msg = shared_handler.get_dict('error_msg')
        self.status_gc = shared_handler.get_dict('status_gc')
        self.now_processing = shared_handler.get_dict('now_processing')
        self.shared_handler = shared_handler
        self.custom_args = {}
        
class handler_args(object):
    def __init__(self,shared_handler,q_name,target_q,global_args,pre_work,do_work,post_work,all_done,forwarder):
        self.target_q = target_q
        self.global_args = global_args
        self.pre_work = pre_work
        self.do_work = do_work
        self.post_work = post_work
        self.all_done = all_done
        self.is_forwarder = forwarder
        self.q_name = q_name

        #custom args
        self.custom_args = {}

        #signal to quit processing
        #TODO: add queue name here
        self.all_done_signal = shared_handler.get_signal(q_name + '_all_done_signal')
        self.all_done_signal.clear()

class Worker(multiprocessing.Process):
    def __init__(self, worker_id, handler, args):
        gen_queue.trace_msg("Init Worker...")
        self.id = worker_id
        self.args = args
        self.args.global_args.now_processing.update({self.id:None})
        super(Worker, self).__init__(target=handler,name=str(worker_id),args=(args,))
        gen_queue.trace_msg("Init Worker...Done.")

    def now_processing(self):
        return self.args.global_args.now_processing.get(self.id)

    def set_now_processing(self,message_id):
        self.args.global_args.now_processing.update({self.id:message_id})

class MessageKilled(Exception):
    pass
   
class gen_queue(object):
    def __init__(self,name, g_args, num_workers):
        self.g_args = g_args
        self._name = name
        self._q = shared_manager.get_queue(self._name)
        self.args = handler_args(
                    g_args.shared_handler,
                    self._name,
                    self._q,
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
        self.id = str(uuid.uuid4())

    def pre_work_handler(self):
        return gen_queue._pre_work

    @classmethod
    def _pre_work(cls,worker,args,item):
        gen_queue.trace_msg("Pre work...")
        try:
            if not args.is_forwarder:
                #if kill request is already made, do not process
                gen_queue.trace_msg("status -> {0}".format(args.global_args.work_status.get(item.id)))
                if args.global_args.work_status.get(item.id) is not ST_KILL():
                    gen_queue.trace_msg("Set Now processing - ")
                    worker.set_now_processing(item.id)
                    args.global_args.work_status.update({item.id:ST_START()})
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
        return args.all_done_signal.is_set()

    def post_work_handler(self):
        return gen_queue._post_work

    @classmethod
    def _post_work(cls,worker,args,item,status, error_msg):
        gen_queue.trace_msg("Post work...")
        try:
            #if not a forwarder, then set status
            if not args.is_forwarder:
                if args.global_args.work_status.get(item.id) is not ST_KILL():
                    args.global_args.work_status.update({item.id:status})
                    args.global_args.error_msg.update({item.id:error_msg})
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
                status = ST_START()
                obj = None
                obj = args.target_q.get(True,5)
                gen_queue.trace_msg('done get queue')
                try:
                    if args.pre_work is not None:
                        args.pre_work(worker,args,obj)
                    gen_queue.trace_msg('about to process')
                    if args.do_work is not None:
                        args.do_work(worker,args,obj)
                    gen_queue.trace_msg('done process')
                    status = ST_SUCCESS()
                except MessageKilled as ex1:
                    status = ST_KILL()
                    err_msg = str(ex1)
                    gen_queue.trace_msg(err_msg, 4)
                    pass
                except Exception as ex:
                    gen_queue.trace_msg('Inner Error Processing...', 4)
                    gen_queue.trace_msg(ex,4)
                    #traceback.print_stack()
                    #set error attributes
                    status = ST_FAIL()
                    err_msg = str(ex)

                finally:
                    if args.post_work is not None:
                        args.post_work(worker,args,obj, status,err_msg)
                    pass

            except queue.Empty:
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
        #_all_q[self.id] = self._q

    def add_worker(self):
        worker_id = str(uuid.uuid4())
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
        gen_queue.trace_msg('enqueue() Called..', 4)
        #create queue item and then enque
        #check for nested queue items
        # check if tracing is set
        item = gen_queue.get_queue_item(obj)

        if self.trace_q is not None:
            self.trace_q.enqueue_async(item)

        gen_queue.enqueue_q(self._q,self.g_args,item)
        return item.id

    def enqueue_async(self, obj):
        return self._enqueue(obj)

    def enqueue_await(self, async_id):
        status = None
        error_msg = None
        while True and async_id is not None:
            try:
                status = self.g_args.work_status.get(async_id)
                if status == ST_START() or status == ST_WAIT():
                    time.sleep(5)
                else:
                    error_msg = self.g_args.error_msg.get(async_id)
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
            self.g_args.work_status.pop(async_id)
            self.g_args.error_msg.pop(async_id)
        except:
            pass
        finally:
            pass
        return (status, error_msg)

    def get_q(self):
        return self._q

    @classmethod
    def get_queue_item(cls,obj):
        queue_chaining = isinstance(obj, queue_item)
        if queue_chaining:
            item = obj
        else:
            item = queue_item(obj)
        return item

    @classmethod
    def enqueue_q(cls,q,g_args,item):
        g_args.work_status.update({item.id:ST_WAIT()})
        g_args.status_gc.update({time.time():item.id})
        q.put(item, False)

    def stop(self):
        #gen_queue._all_queue.remove(self)
        self.args.all_done_signal.set()
        for key, worker in self.workers.items():
            worker.join()

    def kill(self, async_id):
        gen_queue.trace_msg('Kill recieved id {0}'.format(async_id))
        killed = False
        worker_removed = False
        try:
            #find the status of the request
            status = self.g_args.work_status.get(async_id)
            print('status -', status)
            if status == ST_START():
                #find the worker that is processing the message
                for worker_id, w in self.workers.items():
                    print('w - ',w.now_processing())
                    if w.now_processing() == async_id:
                        gen_queue.trace_msg('Killing worker!')
                        self.remove_worker(worker_id)
                        worker_removed = True
                        gen_queue.trace_msg('Kill was successful!')
                        killed = True
                        break
            elif status == ST_WAIT():
                #set the status to kill
                self.g_args.work_status.update({async_id:ST_KILL()})
                gen_queue.trace_msg('Kill request made!')
                killed = True
        except KeyError:
            pass
        except Exception as ex:
            gen_queue.trace_msg('Kill process error!', 4)
            gen_queue.trace_msg(ex, 4)
        finally:
            pass
        if worker_removed:
            self.add_worker()
        return killed

    @classmethod
    def trace_msg(cls,msg, level=0):
        if level >= TRACE_LEVEL():
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
    def _do_work(cls,worker,args,item):
        time.sleep(5)
        gen_queue.trace_msg("my_queue {0}".format(item.item))

class topic_config(object):
    def __init__(self, topic, handler_cls, num_workers):
        self.topic = topic
        self.handler_cls = handler_cls
        self.num_workers = num_workers

class gen_topic_queue(gen_queue):
    def __init__(self, name, g_args, topic_config_arr, num_workers=1):
        super(gen_topic_queue, self).__init__(name,g_args,num_workers)
        #set forwarder
        self.args.is_forwarder = True
        self.args.custom_args['topic_q'] = []
        for topic_config in topic_config_arr:
            #create queues to handle each topic
            q = topic_config.handler_cls(topic_config.topic,g_args, topic_config.num_workers)
            self.args.custom_args['topic_q'].append({topic_config.topic : q})

    def start(self):
        super(gen_topic_queue, self).start()
        for i in range(0,len(self.args.custom_args['topic_q'])):
            list(self.args.custom_args['topic_q'][i].values())[0].start()

    def stop(self):
        for i in range(0,len(self.args.custom_args['topic_q'])):
            list(self.args.custom_args['topic_q'][i].values())[0].stop()
        super(gen_topic_queue, self).stop()

    def do_work_handler(self):
        return gen_topic_queue._do_work

    @classmethod
    def _do_work(cls,worker,args,item):
        orphen_message = True
        topic_q = args.custom_args['topic_q']
        for i in range(0,len(topic_q)):
            p = re.compile(list(topic_q[i].keys())[0].replace(".","[.]"))
            if p.match(item.item['topic']) is not None:
                #queue item to repective queue. honor await request
                list(topic_q[i].values())[0].enqueue_async(item)
                orphen_message = False
                break
        if orphen_message:
            gen_queue.trace_msg("Unhandled topic: {0}".format(item.item['topic']))

class timer_queue(gen_queue):
    def __init__(self,name,g_args,num_seconds,msg,q):
        gen_queue.trace_msg('Timer Init Started...')
        super(timer_queue, self).__init__(name,g_args,1)
        self.args.custom_args['num_seconds'] = num_seconds
        self.args.custom_args['msg'] = msg
        self.args.custom_args['target_q'] = q.get_q()
        self.args.custom_args['self_q'] = self.get_q()
        gen_queue.trace_msg('Timer Initialized...')

    def start(self):
        gen_queue.trace_msg('Timer Starting...')
        super(timer_queue, self).start()
        self.enqueue_async(1)
        gen_queue.trace_msg('Timer Started...')

    def do_work_handler(self):
        return timer_queue._do_work

    @classmethod
    def _do_work(cls,worker,args,item):
        gen_queue.trace_msg('Timer msg delivery...')
        if args.custom_args['target_q'] is not None:
            target_q = args.custom_args['target_q']
            item = gen_queue.get_queue_item(args.custom_args['msg'])
            gen_queue.enqueue_q(target_q,args.global_args,item)
        
        else: print('Target q is None.')

        time.sleep(args.custom_args['num_seconds'])
        #check if we are done.
        if not args.all_done_signal.is_set():        
            item = gen_queue.get_queue_item(1)
            gen_queue.enqueue_q(args.custom_args['self_q'],args.global_args,item)

class trace_queue(gen_queue):
    def do_work_handler(self):
        return trace_queue._do_work

    @classmethod
    def _do_work(cls,worker, args,item):
        gen_queue.trace_msg('{2} id:{0} msg:{1}'.format(item.id,item.item,datetime.datetime.now()))

class gc_queue(gen_queue):
    def __init__(self, name, g_args):
        super(gc_queue, self).__init__(name,g_args,1)

    def do_work_handler(self):
        return gc_queue._do_work

    @classmethod
    def _garbage_collect(cls,args):
        # remove expired status items
        try:
            gc_marker = time.time() - 5
            for gc_key in [t if t < gc_marker else 0 for t in args.global_args.status_gc.keys() ]:
                if gc_key == 0:
                    continue
                async_id = args.global_args.status_gc.get(gc_key)
                if async_id in args.global_args.work_status:
                    gen_queue.trace_msg('Key {0} collected.'.format(async_id))
                    args.global_args.work_status.pop(async_id)
                    if async_id in args.global_args.error_msg:
                        args.global_args.error_msg.pop(async_id)
                
                args.global_args.status_gc.pop(gc_key)
        except Exception as ex:
            print(ex)
            traceback.print_stack()
        finally:
            pass

    @classmethod
    def _do_work(cls,worker,args,item):
        gen_queue.trace_msg('Garbage collecting...')
        if args.global_args.shared_handler.is_distributed():
            print('calling distributed GC!')
            args.global_args.shared_handler.garbage_collect()
        else:
            gc_queue._garbage_collect(args)

class message_obj(object):
    def __init__(self, item, fn):
        self.item = item
        self.fn = fn

class message_loop(gen_queue):
    def __init__(self, name, g_args):
        super(message_loop, self).__init__(name,g_args,1)
        self.args.custom_args['execute_list'] = []
        self.args.custom_args['self_q'] = self.get_q()

    def do_work_handler(self):
        return message_loop._msg_process

    @classmethod
    def _msg_process(cls,worker, args, obj):
        
        #process all the messages
        #add to list except, loop back message
        execute_list = args.custom_args['execute_list']            
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
            item = gen_queue.get_queue_item(message_obj(None,None))
            gen_queue.enqueue_q(args.custom_args['self_q'],args.global_args,item)
            
class kill_topic_handler(gen_queue):
    def do_work_handler(self):
        return kill_topic_handler._do_work

    @classmethod
    def _do_work(cls,worker,args,item):
        v = item.item['msg']
        if v == 'Kill!':
            print("Kill received - {0}".format(v))
            args.global_args.custom_args['kill_client_con'].send(args.global_args.custom_args['app_id'])
        print("kill_topic_handler - {0}".format(v))

class HttpHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith('/' + str(self.server.get_app().get_id())):
            # TODO: check data and support other actions like sending message to a queue
            # content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
            # post_data = self.rfile.read(content_length) # <--- Gets the data itself
            print('Requesting Shutdown...')
            self.server.get_app().message_app_q({'topic':'.q1.kill','msg':'Kill!',})
            self.send_response(200, 'Shutdown request has been sent!')
            self.end_headers()
            print('Requesting Shutdown..Done.')

class MyTCPServer(socketserver.TCPServer):
    def __init__(self,server_address,RequestHandlerClass,bind_and_activate=True,app=None):
        socketserver.TCPServer.__init__(self,server_address,RequestHandlerClass,bind_and_activate)
        self.app = app

    def get_app(self): return self.app

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

class KillThread(threading.Thread):
    def __init__(self, server_con, app):
        threading.Thread.__init__(self)
        self.server_con = server_con
        self.app = app

    def run(self):
        print('Waiting for kill message...')
        msg = self.server_con.recv()
        print("Server is going down, run it again manually!")
        self.app.get_server().shutdown()

class DaemonApp(object):
    def __init__(self, g_args, host='localhost', port=8000,app_q=None):
        self._server_address = (host, port)
        self._httpd = MyTCPServer(self._server_address, HttpHandler, True,self)
        self._id = str(uuid.uuid4())
        self.server_con, self.client_con = multiprocessing.Pipe(False)
        self.g_args = g_args
        g_args.custom_args['kill_client_con'] = self.client_con
        g_args.custom_args['app_id'] = self.get_id()
        g_args.custom_args['server_address'] = self._server_address
        if app_q is not None:
            g_args.custom_args['app_q'] = app_q.get_q()
        
    def get_server(self): return self._httpd

    def get_id(self): return self._id
    def get_server_address(self): return self._server_address

    def message_app_q(self, msg):
            item = gen_queue.get_queue_item(msg)
            gen_queue.enqueue_q(self.g_args.custom_args['app_q'],self.g_args,item)

    def start_app(self):
        print('Starting Daemon.... id:{0}'.format(self.get_id()))
        kill_thread = KillThread(self.server_con, self)
        kill_thread.start()
        self.get_server().serve_forever()
        kill_thread.join()
        pass

    def stop_app(self):
        pass

    pass

class shared_manager(object):
    _distributed = None
    _local_manager = None
    _dist_manager = None

    def __init__(self):
        raise Exception('Shared_manager() class object cannot be instatiated.')

    @classmethod
    def is_distributed(cls):
        return True
   
    @classmethod
    def dist_connect(cls):
        AppSharedObjectClient.setup()
        while True:
            m = AppSharedObjectClient(address=_distributed_server_address, authkey=b'abracadabra')
            try:
                m.connect()
                shared_manager._dist_manager = m
                #got good connection break
                m.register_me(os.uname()[1],{})
                gen_queue.trace_msg('Connected to master node.', 2)
                break
            except ConnectionRefusedError as ex:
                #wait and try again
                gen_queue.trace_msg('Waiting for master node.', 2)
                time.sleep(10)
            except Exception as ex1:
                gen_queue.trace_msg(ex1, 4)
                traceback.print_stack()
            finally:
                pass

    @classmethod
    def set_distributed(cls,distributed=False):
        if  not distributed:
            shared_manager._local_manager = Manager()
        else:
            shared_manager.dist_connect()

    @classmethod
    def get_queue(cls, name):
        q = None
        if not shared_manager.is_distributed():
            q = shared_manager._local_manager.Queue()
        else:
            if shared_manager._dist_manager is None:
                shared_manager.dist_connect()
            q = shared_manager._dist_manager.get_queue(name)

        return q

    @classmethod
    def get_dict(cls, name):
        d = None
        if not shared_manager.is_distributed():
            d = shared_manager._local_manager.dict()
        else:
            if shared_manager._dist_manager is None:
                shared_manager.dist_connect()
            d = shared_manager._dist_manager.get_dict(name)

        return d

    @classmethod
    def get_signal(cls, name):
        s = None
        if not shared_manager.is_distributed():
            s = shared_manager._local_manager.Event()
        else:
            if shared_manager._dist_manager is None:
                shared_manager.dist_connect()
            s = shared_manager._dist_manager.get_signal(name)
        return s

    @classmethod
    def garbage_collect(cls):
        if shared_manager._dist_manager is None:
            shared_manager.dist_connect()
        shared_manager._dist_manager.garbage_collect()

if __name__ == '__main__':
    print('Main called...')
    shared_manager.set_distributed(True)
    q = shared_manager.get_queue('hello')
    g_args = global_args(shared_manager)    
    try:
        s = my_queue('q1',g_args,1)
        s.start()
        s.enqueue_async(2)
        s.enqueue_async(3)
        s.stop()
    except Exception as ex:
        print(ex)
