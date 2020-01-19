from Frontier import Frontier
from Fetcher import Fetcher
import redis

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

url_frontier = Frontier(redis_client=r)
url_frontier.add_seed(
    ['https://unsplash.com/', 'https://news.ycombinator.com/'])
url_frontier.start_seeding()

pipelines = url_frontier.get_frontier()

fetcher = Fetcher(redis_client=r)
for pipeline in pipelines:
    fetcher.fetch(pipeline)
