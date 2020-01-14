import requests
import redis
from concurrent.futures import ThreadPoolExecutor
import sys

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

urls = [
    'https://www.3dbuzz.com/',
    'https://www.wsj.com/articles/visa-nears-deal-to-buy-fintech-startup-plaid-11578948426',
    'https://appleinsider.com/articles/20/01/13/app-tracking-alert-in-ios-13-has-dramatically-cut-location-data-flow-to-ad-industry',
    'http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.86.6185&rep=rep1&type=pdf',
    'https://whatismusic.info/blog/AUnifiedTheoryOfMusicAndDreaming.html',
    'https://blog.samaltman.com/how-to-invest-in-startups'
]

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def populate_redis(urls):
    r.lpush('todo', *urls)

# WE will use list for the todo_pipe and list for the done pipe


class Fetcher():
    def __init__(self, todo_pipe='todo', done_pipe='done', batch_size=16):
        self.todo_pipe = todo_pipe
        self.done_pipe = done_pipe
        self.batch_size = batch_size
        self._batch = []

    def fetch(self):
        self._read_queue()
        print(f'Starting to download a batch of {len(self._batch)} items\n\n')
        results = []
        with ThreadPoolExecutor() as executor:
            results = executor.map(self._make_request, urls)
        self._write_to_queue(results)
        print(f'\n\nDownloaded batch of {len(self._batch)} items')

    def _read_queue(self):
        self._batch = list(map(lambda x: x.decode(
            'utf-8'), r.lrange(self.todo_pipe, 0, self.batch_size)))
        r.ltrim(self.todo_pipe, self.batch_size, r.llen(self.todo_pipe))

    def _write_to_queue(self, results):
        for res in results:
            print(
                f'Downloaded \033[32m{round(sys.getsizeof(res["content"]) / (1024**2), 2)} MB\033[m from \033[32m{res["url"]}\033[m')

    def _make_request(self, url):
        res = requests.get(url)
        return {'content': res.content, 'url': url}


fetcher = Fetcher()
populate_redis(urls)
fetcher.fetch()
