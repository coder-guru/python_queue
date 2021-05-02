import unittest
import traceback
import uuid

from multiprocessing.managers import BaseManager
class QueueManager(BaseManager):
    def setup():
        QueueManager.register('get_queue')
        QueueManager.register('get_dict')
        QueueManager.register('get_signal')
        QueueManager.register('register_me')

class AppQueueManager(QueueManager):
    pass
    def setup():
        QueueManager.setup()
        AppQueueManager.register('garbage_collect')
        pass

class queue_item(object):
    def __init__(self, item,id=None):
        self.item = item
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id

class TestDistQueue(unittest.TestCase):

    def test_send(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            queue = m.get_queue('my_q')
            queue.put('Hello, World!')
            print('putting queue item')
            item = queue_item('Hello')
            queue.put(item)
            
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_dict_put(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            d = m.get_dict('my_q')
            d.update({'msg':'Hello, Dict!'})
            d.update({'msg2':'Hello, Dict2!'})
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_dict_get(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            d = m.get_dict('my_q')
            print(d.get('msg'))
            print(d.get('msg2'))
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_receive(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            queue = m.get_queue('my_q')
            print(queue.get())
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_signal_set(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            s = m.get_signal('all_done')
            if s.is_set():
                s.clear()
            else:
                s.set()
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_signal_wait(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            s = m.get_signal('all_done')
            print(s.is_set())
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

    def test_send_new(self):
        try:
            AppQueueManager.setup()
            m = AppQueueManager(address=('localhost', 50000), authkey=b'abracadabra')
            m.connect()
            queue = m.get_queue('my_q')
            queue.put('Hello, World!')
            m.garbage_collect()
            
        except Exception as ex:
            print(ex)
            self.fail(traceback.print_stack())

if __name__ == '__main__':
    #unittest.main()
    test = TestDistQueue()
    test.test_send()

