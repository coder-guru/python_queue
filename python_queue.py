"""  
TODO: One master queue , process on topic
"""
import traceback
import multiprocessing
from multiprocessing import Manager
import Queue
import time
import re
import random

_manager = Manager()
class queue_item(object):
    def __init__(self, item, await_support):
        self.item = item
        self.done = None
        if await_support:
            self.done = _manager.Semaphore(1)

class gen_queue(object):
    def __init__(self,num_workers):
        self.q = multiprocessing.Queue()
        self.all_done = multiprocessing.Value('b', 0)
        self.workers = []
        if num_workers <= 0:
            num_workers = 1
        self.num_workers = num_workers

    # indicate whether this queue is forwarder(no real processing of data)
    def is_forwarder(self):
        return False

    def get_processor(self):
        return self.__process
        
    def start(self):
        for i in range(0,self.num_workers):      
            p = multiprocessing.Process(target=gen_queue.__consume, args=(self.q,self.all_done,self.get_processor(),self.is_forwarder()))
            self.workers.append(p)
            p.start()

    def enqueue(self, obj, await=False):
        #create queue item and then enque
        #check for nested queue items
        queue_chaining = isinstance(obj, queue_item)
        if queue_chaining:
            item = obj
        else:
            item = queue_item(obj, await)
        if item.done is not None and not queue_chaining:
            item.done.acquire()
        self.q.put(item, False)
        if item.done is not None and not queue_chaining:
            item.done.acquire()

    def stop(self):
        self.all_done.value = 1
        for i in range(0,self.num_workers):      
            self.workers[i].join()

    def __process(self, obj):
        print("Consume {0}".format(obj))
        
    @classmethod
    def __consume(cls,q, all_done, f_process,forwarder=False):
        while True:
            try:
                obj = q.get(True,5)
                try:
                    f_process(obj)
                except Exception as ex:
                    print('Error Processing..')
                    print(ex)
                finally:
                    if obj.done is not None and not forwarder:
                        obj.done.release()
                    pass

            except Queue.Empty:
                if all_done.value == 1:
                    print('Breaking out...')
                    break 
            except Exception as ex:
                print('Error')
                print(ex)
                traceback.print_stack()
                break
            finally:
                pass

class my_queue(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        time.sleep(random.randint(1,5))
        print("my_queue {0}".format(obj.item))

class topic_config(object):
    def __init__(self, topic, handler_cls, num_workers):
        self.topic = topic
        self.handler_cls = handler_cls
        self.num_workers = num_workers

class gen_topic_queue(gen_queue):
    def __init__(self, topic_config_arr, num_workers=1):
        super(gen_topic_queue, self).__init__(num_workers)
        self.__topic_q = []
        for topic in topic_config_arr:
            #create queues to handle each topic
            q = topic.handler_cls(topic.num_workers)
            self.__topic_q.append({topic.topic : q})

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
        for i in range(0,len(self.__topic_q)):
            p = re.compile(self.__topic_q[i].keys()[0].replace(".","[.]"))
            if p.match(obj.item['topic']) is not None:
                #queue item to repective queue. honor await request
                self.__topic_q[i].values()[0].enqueue(obj, False)
                break

class timer_queue(gen_queue):
    def __init__(self, num_seconds, msg, q):
        self.num_seconds = num_seconds
        self.msg = msg
        self.target_q = q
        super(timer_queue, self).__init__(1)

    def start(self):
        super(timer_queue, self).start()
        self.enqueue(1, False)

    def get_processor(self):
        return self.__process

    def __process(self,obj):
        print('Timer msg delivery...')
        self.target_q.enqueue(self.msg, False)
        time.sleep(self.num_seconds)
        if not self.all_done.value == 1:
            self.enqueue(1, False)

