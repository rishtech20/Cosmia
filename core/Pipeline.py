class Pipeline():
    def __init__(self, name, redis_client):
        self.name = name
        self.r = redis_client

    def push(self, data):
        self.r.lpush(self.name, *data)

    def pop(self, size):
        popped_data = list(map(lambda x: x.decode(
            'utf-8'), self.r.lrange(self.name, 0, size)))
        self.r.ltrim(self.name, size, self.r.llen(self.name))
        return popped_data

    def flush(self):
        self.r.delete(self.name)
