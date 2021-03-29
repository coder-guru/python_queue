"""  
TODO: AsynAwait execution
TODO: One master queue , process on topic
TODO: processing, on another process
TODO: topic pattern match
TODO: FIFO processing, option
"""

import multiprocessing
from multiprocessing import Manager
import Queue
import time
import re

class queue_item(object):
    def __init__(self, item):
        self.item = item
        self.done = Manager().Semaphore(1)

class gen_queue(object):
    def __init__(self,num_workers):
        self.q = multiprocessing.Queue()
        self.all_done = multiprocessing.Value('b', 0)
        self.workers = []
        if num_workers <= 0:
            num_workers = 1
        self.num_workers = num_workers

    def get_processor(self):
        return self.__process
        
    def start(self):
        for i in range(0,self.num_workers):      
            p = multiprocessing.Process(target=gen_queue.__consume, args=(self.q,self.all_done,self.get_processor(),))
            self.workers.append(p)
            p.start()

    def enqueue(self, obj, await=False):
        #create queue item and then enque
        item = queue_item(obj)
        item.done.acquire()
        self.q.put(item, False)
        if await:
            item.done.acquire()

    def stop(self):
        self.all_done.value = 1
        for i in range(0,self.num_workers):      
            self.workers[i].join()

    def __process(self, obj):
        print("Consume {0}".format(obj))
        
    @classmethod
    def __consume(cls,q, all_done, f_process):
        while True:
            try:
                obj = q.get(True,5)
                try:
                    f_process(obj)
                except:
                    pass
                finally:
                    obj.done.release()

            except Queue.Empty:
                if all_done.value == 1:
                    print('Breaking out...')
                    break 
            except Exception as ex:
                print('Error')
                print(ex)
                break
            finally:
                pass

class my_queue(gen_queue):
    def get_processor(self):
        return self.__process

    def __process(self, obj):
        print("my_queue {0}".format(obj))

class topic_config(object):
    def __init__(self, topic, handler_cls, num_workers):
        self.topic = topic
        self.handler_cls = handler_cls
        self.num_workers = num_workers

class gen_topic_queue(gen_queue):
    def __init__(self, topic_config_arr):
        super(gen_topic_queue, self).__init__(1)
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

    def get_processor(self):
        return self.__process

    def __process(self,obj):
        #match topic and send to associated queue
        for i in range(0,len(self.__topic_q)):
            p = re.compile(self.__topic_q[i].keys()[0].replace(".","[.]"))
            if p.match(obj.item['topic']) is not None:
                self.__topic_q[i].values()[0].enqueue(obj)
                break

def main():
    print('Hello, World!')
    s = my_queue(3)
    s.start()
    s.enqueue(2)
    s.enqueue(3)
    s.enqueue(4)
    s.enqueue(5)
    s.stop()
    l = []
    l.append(topic_config('.q1.*', my_queue, 1))
    l.append(topic_config('.q2.*', my_queue, 1))
    g = gen_topic_queue(l)
    g.start()
    g.enqueue({'topic':'.q1.data','process_name':'MY_Q1_PROCESS'})
    g.enqueue({'topic':'.q2.data','process_name':'MY_q2_PROCESS'}, True)
    g.stop()
    
if __name__ == "__main__":
    main()
