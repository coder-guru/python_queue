"""  
TODO: Support Kill
TODO: Support sending message to main publish queue
"""
import traceback, sys
import multiprocessing
from multiprocessing import Manager
import Queue
import time, datetime
import re
import random
import uuid

_manager = Manager()

class message_status(object):
    def __init__(self, success, error_msg):
        self.success = success
        self.error_msg = error_msg

class queue_item(object):
    def __init__(self, key, item, await_support):
        if key is None:
            key = hash(item)
        self.key = key
        self.item = item
        self.done = None
        self.id = uuid.uuid4()
        if await_support:
            self.done = _manager.Semaphore(1)

class gen_queue(object):
    #keep all instance of queue (for broadcast type of processing)
    _all_queue = []

    def __init__(self,num_workers):
        self.q = multiprocessing.Queue()
        self.all_done = multiprocessing.Value('b', 0)
        self.workers = {}
        self.work_status = _manager.dict()

        if num_workers <= 0:
            num_workers = 1
        self.num_workers = num_workers
        self.trace_q = None
        #create a lock for protected access to workers list
        self.lock = multiprocessing.Lock()

    # indicate whether this queue is forwarder(no real processing of data)
    def is_forwarder(self):
        return False

    def get_processor(self):
        return self.__process
        
    def start(self):
        for i in range(0,self.num_workers):      
            self.add_worker()
        #add to global list
        gen_queue._all_queue.append(self)

    def add_worker(self):
        worker_id = uuid.uuid4()
        p = multiprocessing.Process(target=gen_queue.__consume, args=(worker_id, self.q,self.all_done \
                                ,self.get_processor(),self.is_forwarder(),self._start_process, self._end_process))
        self.workers[worker_id] = {'worker':p,}
        self.work_status[worker_id] = _manager.dict()
        p.start()

    def remove_worker(self, worker_id):
        try:
            worker = self.workers[worker_id]
            self.workers.pop(worker_id)
            self.work_status.pop(worker_id)
            worker['worker'].terminate()
            self.add_worker()
        except Exception as ex:
            gen_queue.trace_msg('remove_worker() failed.')
            gen_queue.trace_msg(ex)
        finally:
            pass

    def set_trace(self, trace_q):
        self.trace_q = trace_q

    def enqueue(self, key, obj, await=False):
        #create queue item and then enque
        #check for nested queue items
        queue_chaining = isinstance(obj, queue_item)
        if queue_chaining:
            item = obj
        else:
            item = queue_item(key, obj, await)
        # check if tracing is set
        if self.trace_q is not None:
            self.trace_q.enqueue(None,item, False)

        if item.done is not None and not queue_chaining:
            item.done.acquire()
        self.q.put(item, False)
        if item.done is not None and not queue_chaining:
            item.done.acquire()
        #return status
        #return item.status.value

    def stop(self):
        gen_queue._all_queue.remove(self)
        self.all_done.value = 1
        for key, worker in self.workers.items():      
            worker['worker'].join()

    def _start_process(self, worker_id, key):
        try:
            self.lock.acquire()
            self.work_status[worker_id].clear()
            if key is not None:
                self.work_status[worker_id][key] = 0
        except Exception as ex:
            gen_queue.trace_msg('_start_process() ', ex)
        finally:
            self.lock.release()

    def _end_process(self, worker_id, key, status=0):
        try:
            self.lock.acquire()
            self.work_status[worker_id][key] = status
        except Exception as ex:
            gen_queue.trace_msg('_end_process() ', ex)
        finally:
            self.lock.release()

    def kill(self, key):
        gen_queue.trace_msg('Kill recieved key {0}'.format(key))
        killed = False
        self.lock.acquire()
        try:
            #find the worker that is processing
            for worker_id, status in self.work_status.items():
                if status is not None:
                    if status.get(key, None) is not None: 
                        self.remove_worker(worker_id)
                        gen_queue.trace_msg('Kill was successful!')
                        killed = True
                        break
        except Exception as ex:
            gen_queue.trace_msg('Kill process error!', ex)
        finally:
            self.lock.release()
        if killed:
            self.add_worker()
        return killed

    @classmethod
    def trace_msg(cls,msg, level=0):
        if level > 0:
            print(msg)
        pass

    @classmethod
    def all_queue_kill(cls,key):
        for q in gen_queue._all_queue:
            try:
                q.kill(key)
                pass
            except Exception as ex:
                gen_queue.trace_msg(ex)
            finally:
                pass
        pass

    def __process(self, obj):
        gen_queue.trace_msg("Consume {0}".format(obj))
        
    @classmethod
    def __consume(cls,worker_id,q, all_done, f_process,forwarder=False,f_start=None,f_end=None):
        f_start = None
        f_end = None
        while True:
            try:
                gen_queue.trace_msg('about to get queue')
                obj = q.get(True,5)
                gen_queue.trace_msg('done get queue')
                try:
                    if f_start is not None:
                        f_start(worker_id,obj.key)
                    gen_queue.trace_msg('about to process')
                    f_process(obj)
                    gen_queue.trace_msg('done process')
                    if f_end is not None:
                        f_end(worker_id, obj.key, 0)
                except Exception as ex:
                    gen_queue.trace_msg('Inner Error Processing...', 4)
                    gen_queue.trace_msg(ex,4)
                    #set error attributes
                    if f_end is not None:
                        f_end(worker_id, obj.key, 1)
                finally:
                    if obj.done is not None and not forwarder:
                        obj.done.release()
                    pass

            except Queue.Empty:
                gen_queue.trace_msg('Queue Empty...')
                if all_done.value == 1:
                    gen_queue.trace_msg('Breaking out...')
                    break 
            except Exception as ex:
                gen_queue.trace_msg('Error Processing...', 4)
                gen_queue.trace_msg(ex,4)
                traceback.print_stack()
                break
            finally:
                pass

class my_queue(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        time.sleep(random.randint(1,5))
        gen_queue.trace_msg("my_queue {0}".format(obj.item))

class topic_config(object):
    def __init__(self, topic, handler_cls, num_workers):
        self.topic = topic
        self.handler_cls = handler_cls
        self.num_workers = num_workers

class gen_topic_queue(gen_queue):
    def __init__(self, topic_config_arr, num_workers=1):
        super(gen_topic_queue, self).__init__(num_workers)
        self.__topic_q = []
        for topic_config in topic_config_arr:
            #create queues to handle each topic
            q = topic_config.handler_cls(topic_config.num_workers)
            self.__topic_q.append({topic_config.topic : q})

    def start(self):
        super(gen_topic_queue, self).start()
        for i in range(0,len(self.__topic_q)):
            self.__topic_q[i].values()[0].start()

    def stop(self):
        for i in range(0,len(self.__topic_q)):
            self.__topic_q[i].values()[0].stop()
        super(gen_topic_queue, self).stop()

    def is_forwarder(self):
        return True

    def get_processor(self):
        return self.__process

    def __process(self,obj):
        #match topic and send to associated queue
        orphen_message = True
        for i in range(0,len(self.__topic_q)):
            p = re.compile(self.__topic_q[i].keys()[0].replace(".","[.]"))
            if p.match(obj.item['topic']) is not None:
                #queue item to repective queue. honor await request
                self.__topic_q[i].values()[0].enqueue(obj.key, obj, False)
                orphen_message = False
                break
        if orphen_message:
            gen_queue.trace_msg("Unhandled topic: {0}".format(obj.item['topic']))

class timer_queue(gen_queue):
    def __init__(self, num_seconds, msg, q):
        self.num_seconds = num_seconds
        self.msg = msg
        self.target_q = q
        super(timer_queue, self).__init__(1)

    def start(self):
        super(timer_queue, self).start()
        self.enqueue(None, 1, False)

    def get_processor(self):
        return self.__process

    def __process(self,obj):
        gen_queue.trace_msg('Timer msg delivery...')
        self.target_q.enqueue(1, self.msg, False)
        time.sleep(self.num_seconds)
        if not self.all_done.value == 1:
            self.enqueue(1,1, False)

class trace_queue(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self,obj):
        gen_queue.trace_msg('{2} id:{0} msg:{1}'.format(obj.id,obj.item,datetime.datetime.now()))

class message_obj(object):
    def __init__(self, item, fn):
        self.item = item
        self.fn = fn

#message handler base class
class message_handler(object):
    def handle(self, msg):
        raise NotImplementedError       
class message_loop(gen_queue):
    def __init__(self):
        self.execute_list = []
        super(message_loop, self).__init__(1)

    def get_processor(self):
        return self.__process

    def __process(self,obj):
        #process all the messages
        #add to list except, loop back message
        if obj is None:
            gen_queue.trace_msg('object is none')
        if obj.item.fn is not None:
            try:
                self.execute_list.append(obj.item.fn.handle(obj.item.item))
            except Exception as ex:
                gen_queue.trace_msg('Error Messsage_loop __Process()')
                gen_queue.trace_msg(ex)
            finally:
                pass

        # process all
        for generator in self.execute_list:
            if obj is not None:
                try:
                    next(generator)
                except StopIteration:
                    self.execute_list.remove(generator)
                finally:
                    pass
        # add loop back message, if there is pending messages to process
        if len(self.execute_list) > 0:
            gen_queue.trace_msg('loop enqueue')
            self.enqueue(1, message_obj(None,None), False)

