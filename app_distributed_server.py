import time
from app_dist_objects import queue_item, handle_msg
from shared_object_manager import SharedObjectManager
from app_shared_object_manager import AppSharedObjectManager

_host_address = ('localhost', 50000)

def start():
    AppSharedObjectManager.setup()
    m = AppSharedObjectManager(address=_host_address, authkey=b'abracadabra')

    s = m.get_server()
    #m.start()
    #ss = m.get_queue('hi')
    print('Queue Master started...')
    s.serve_forever()
    #m.shutdown()

if __name__ == '__main__':
    start()
