# refer: https://dramatiq.io/guide.html

# modules
from dramatiq.middleware import TimeLimitExceeded
from dramatiq.brokers.redis import RedisBroker
import dramatiq
import sys
import os

# main

# read the url of the redis-server
url= os.getenv('REDIS_URL')
if url is None:
    print('ERROR: REDIS_URL is not set.')
    sys.exit(1)

# refer: https://dramatiq.io/reference.html
try:
    broker= RedisBroker(url= url)
    dramatiq.set_broker(broker)
except Exception as e:
    print(f'ERROR: details : {str(e)}')
    sys.exit(1)


# this is a dummy background-process
@dramatiq.actor(max_age= 600000, time_limit= 120000) # the time is stated in milli-seconds: (10, 2) min
def execute(user_id : str, vector : list[int]):
    # desc.: this function is executed by the 'dramatiq worker'
    try:
        s= sum(vector)
    except TimeLimitExceeded as e:
        print(f'ERROR: TimeLimitExceeded. Details: {e}')
    except Exception as e:
        print(f'ERROR: failed to execute task. Details: {e}')
    else:
        print( f'{user_id} : {s}' )
