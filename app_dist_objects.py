import uuid

class queue_item(object):
    def __init__(self, item,id=None):
        self.item = item
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id

def handle_msg(msg):
        for i in range(msg * 5,(msg + 1) * 5):
            print('loop {0}'.format(i))
            yield
