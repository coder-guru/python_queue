import time
from shared_object_manager import SharedObjectManager, SharedObjectImpl

class AppSharedObjectManager(SharedObjectManager):
    pass
    def garbage_collect():
        print('Garbage collecting...')
        gc_marker = time.time() - 5
        work_status = SharedObjectImpl.get_dict('work_status')
        error_msg = SharedObjectImpl.get_dict('error_msg')
        status_gc = SharedObjectImpl.get_dict('status_gc')
        for gc_key in [t if t < gc_marker else 0 for t in status_gc.keys() ]:
            if gc_key == 0:
                continue
            async_id = status_gc.get(gc_key)
            if async_id in work_status:
                print('Key {0} collected.'.format(async_id))
                work_status.pop(async_id)
                if async_id in error_msg:
                    error_msg.pop(async_id)
            
            status_gc.pop(gc_key)
        pass

    def setup():
        SharedObjectManager.setup()
        AppSharedObjectManager.register('garbage_collect', callable=AppSharedObjectManager.garbage_collect)
        pass
