import requests
import redis
from concurrent.futures import ThreadPoolExecutor
import sys


class Fetcher():
    def __init__(self, done_pipe='done', batch_size=16, redis_client=None):
        self.done_pipe = done_pipe
        self.batch_size = batch_size
        self._batch = []

    def fetch(self, pipeline):
        self._read_queue(pipeline)
        print(f'Starting to download a batch of {len(self._batch)} items\n\n')
        results = []
        with ThreadPoolExecutor() as executor:
            results = executor.map(self._make_request, self._batch)
        self._write_to_queue(results)
        print(f'\n\nDownloaded batch of {len(self._batch)} items')

    def _read_queue(self, pipeline):
        self._batch = pipeline.pop(size=self.batch_size)

    def _write_to_queue(self, results):
        for res in results:
            print(
                f'Downloaded \033[32m{round(sys.getsizeof(res["content"]) / (1024**2), 2)} MB\033[m from \033[32m{res["url"]}\033[m')

    def _make_request(self, url):
        res = requests.get(url)
        return {'content': res.content, 'url': url}
