import redis
import re
import requests
from Pipeline import Pipeline

rhref = re.compile(r'<[aA].*(href|HREF)=([^\s>]+)')


class Frontier():
    def __init__(self, seed_key="seed_urls", redis_client=None):
        self.seed_key = seed_key
        self.crawl_frontier = []
        self.r = redis_client

    def add_seed(self, seed_urls):
        self.r.sadd(self.seed_key, *set(seed_urls))
        for url in seed_urls:
            self.crawl_frontier.append(Pipeline(name=url, redis_client=self.r))

    def start_seeding(self):
        for pipeline in self.crawl_frontier:
            res = requests.get(pipeline.name)
            links = rhref.findall(res.text)
            for link in links:
                link = link[1].replace("\'", '').replace('"', "").split("#")[0]
                if link.startswith("mailto:"):
                    continue
                elif link[:4] == 'http':
                    if link.startswith(pipeline.name):
                        pipeline.push([link])
                else:
                    if link.startswith('/'):
                        pipeline.push([pipeline.name.rstrip('/') + link])
                    else:
                        pipeline.push([pipeline.name.rstrip('/') + '/' + link])

    def get_seed(self):
        return list(map(lambda x: x.decode(
            'utf-8'), self.r.smembers(self.seed_key)))

    def remove_seed(self, seed_urls):
        for pipeline in self.crawl_frontier:
            if pipeline.name in seed_urls:
                pipeline.flush()
        return self.r.srem(self.seed_key, *set(seed_urls))

    def get_frontier(self):
        return self.crawl_frontier

    def flush_frontier(self):
        for pipeline in self.crawl_frontier:
            pipeline.flush()

        return 'flushed'
