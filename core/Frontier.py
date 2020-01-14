import redis
import re
import requests
from Pipeline import Pipeline

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

rhref = re.compile(r'<[aA].*(href|HREF)=([^\s>]+)')


class Frontier():
    def __init__(self, seed_key="seed_urls"):
        self.seed_key = seed_key
        self.crawl_frontier = []

    def add_seed(self, seed_urls):
        r.sadd(self.seed_key, *set(seed_urls))
        for url in seed_urls:
            self.crawl_frontier.append(Pipeline(name=url, redis_client=r))

    def start_seeding(self):
        for pipeline in self.crawl_frontier:
            res = requests.get(pipeline.name)
            links = rhref.findall(res.text)
            print(links)
            for link in links:
                link = link[1].replace("\'", '').replace('"', "").split("#")[0]
                if link.startswith("mailto:"):
                    continue
                elif link[:4] == 'http':
                    if link.startswith(pipeline.name):
                        # r.lpush(self.crawl_frontier, link)
                        pipeline.push([link])
                else:
                    if link.startswith('/'):
                        # r.lpush(self.crawl_frontier,
                        #         pipeline.name.rstrip('/') + link)
                        pipeline.push([pipeline.name.rstrip('/') + link])
                    else:
                        # r.lpush(self.crawl_frontier,
                        #         pipeline.name.rstrip('/') + '/' + link)
                        pipeline.push([pipeline.name.rstrip('/') + '/' + link])

    def get_seed(self):
        return list(map(lambda x: x.decode(
            'utf-8'), r.smembers(self.seed_key)))

    def remove_seed(self, seed_urls):
        for pipeline in self.crawl_frontier:
            if pipeline.name in seed_urls:
                pipeline.flush()
        return r.srem(self.seed_key, *set(seed_urls))

    def get_frontier(self):
        return self.crawl_frontier

    def flush_frontier(self):
        for pipeline in self.crawl_frontier:
            pipeline.flush()

        return 'flushed'


frontier = Frontier()
frontier.flush_frontier()
frontier.add_seed(['https://news.ycombinator.com/',
                   'https://blog.rishabhmadan.com'])
print(frontier.get_seed())
frontier.start_seeding()
