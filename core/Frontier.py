import redis
import re
import requests

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

rhref = re.compile(r'<[aA].*(href|HREF)=([^\s>]+)')


class Frontier():
    def __init__(self, seed_key="seed_urls", crawl_frontier='todo'):
        self.seed_key = seed_key
        self.crawl_frontier = crawl_frontier

    def add_seed(self, seed_urls):
        r.sadd(self.seed_key, *set(seed_urls))

    def start_seeding(self):
        url_list = list(map(lambda x: x.decode(
            'utf-8'), r.smembers(self.seed_key)))

        for url in url_list:
            res = requests.get(url)
            links = rhref.findall(res.text)

            for link in links:
                link = link[1].replace("\'", '').replace('"', "").split("#")[0]
                if link.startswith("mailto:"):
                    continue
                if link[:4] == 'http':
                    if link.startswith(url):
                        r.lpush(self.crawl_frontier, link)
                else:
                    if link.startswith('/'):
                        r.lpush(self.crawl_frontier, url.rstrip('/') + link)
                    else:
                        r.lpush(self.crawl_frontier,
                                url.rstrip('/') + '/' + link)

    def get_seed(self):
        return list(map(lambda x: x.decode(
            'utf-8'), r.smembers(self.seed_key)))

    def remove_seed(self, seed_urls):
        return r.srem(self.seed_key, *set(seed_urls))

    def get_frontier(self):
        pass

    def flush_frontier(self):
        pass


frontier = Frontier()

frontier.add_seed(['https://news.ycombinator.com/',
                   'https://blog.rishabhmadan.com'])

print(frontier.get_seed())

frontier.remove_seed(['https://news.ycombinator.com/',
                      'https://blog.rishabhmadan.com'])

print(frontier.get_seed())
