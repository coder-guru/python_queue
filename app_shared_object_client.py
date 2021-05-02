from multiprocessing.managers import BaseManager

class AppSharedObjectClient(BaseManager):
    def setup():
        AppSharedObjectClient.register('get_queue')
        AppSharedObjectClient.register('get_dict')
        AppSharedObjectClient.register('get_signal')
        AppSharedObjectClient.register('register_me')
        AppSharedObjectClient.register('garbage_collect')
        pass