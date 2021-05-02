from multiprocessing.managers import BaseManager
from queue import Queue
import multiprocessing
from threading import Lock, Event
import socketserver

class SharedObjectImpl(object):
    _q_master = dict()
    _dict_master = dict()
    _signal_master = dict()
    _node_master = dict()
    _lock = None

    def new_queue(name):
        q = None
        print('New Queue created!')
        q = Queue()
        return q

    def new_dict(name):
        d  = None
        print('New Dict created!')
        d = dict()
        return d

    def new_signal(name):
        s  = None
        print('New Event created!')
        s = Event()
        return s

    def get_queue(name):
        q = None
        SharedObjectImpl._lock.acquire()
        try:
            if name in SharedObjectImpl._q_master.keys():
                print('get_queue()!')
                q = SharedObjectImpl._q_master[name]
            else:
                print('New queue : get_queue()!')
                q = SharedObjectImpl.new_queue(name)
                SharedObjectImpl._q_master[name] = q

        finally:
            SharedObjectImpl._lock.release()

        return q

    def get_dict(name):
        d = None
        SharedObjectImpl._lock.acquire()
        try:
            if name in SharedObjectImpl._dict_master.keys():
                print('get_dict()!')
                d = SharedObjectImpl._dict_master[name]
            else:
                print('New dict : get_dict()!')
                d = SharedObjectImpl.new_dict(name)
                SharedObjectImpl._dict_master[name] = d

        finally:
            SharedObjectImpl._lock.release()

        return d

    def get_signal(name):
        s = None
        SharedObjectImpl._lock.acquire()
        try:
            if name in SharedObjectImpl._signal_master.keys():
                print('get_signal()!')
                s = SharedObjectImpl._signal_master[name]
            else:
                print('New Signal : get_signal()!')
                s = SharedObjectImpl.new_signal(name)
                SharedObjectImpl._signal_master[name] = s

        finally:
            SharedObjectImpl._lock.release()

        return s

    def register_me(name,prop):
        SharedObjectImpl._lock.acquire()
        try:
            SharedObjectImpl._node_master[name] = prop
            print(f'Node {name} connected.')
        finally:
            SharedObjectImpl._lock.release()

    def setup():
        SharedObjectImpl._lock = Lock()

class SharedObjectManager(BaseManager):

    def get_queue(name):
        return SharedObjectImpl.get_queue(name)

    def get_dict(name):
        return SharedObjectImpl.get_dict(name)

    def register_me(name,prop):
        SharedObjectImpl.register_me(name,prop)

    def get_signal(name):
        return SharedObjectImpl.get_signal(name)

    def setup():
        socketserver.BaseServer.allow_reuse_address = True
        SharedObjectImpl.setup()
        SharedObjectManager.register('get_queue', callable=SharedObjectManager.get_queue)
        SharedObjectManager.register('get_dict', callable=SharedObjectManager.get_dict)
        SharedObjectManager.register('register_me', callable=SharedObjectManager.register_me)
        SharedObjectManager.register('get_signal', callable=SharedObjectManager.get_signal)


   