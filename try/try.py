import asyncio
import datetime
import time
from time import sleep

import pytz


async def async_hello_world():
    now = time.time()
    await asyncio.sleep(1)
    print(time.time() - now)
    print("Hello, world!")
    await asyncio.sleep(1)
    print(time.time() - now)


def conv_time(ct):
    '''
    conv_time(1476374400000) --> '20161014000000.000'
    '''
    local_time = time.localtime(ct / 1000)
    data_head = time.strftime('%Y%m%d%H%M%S', local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = '%s.%03d' % (data_head, data_secs)
    return time_stamp


async def main():
    # tasks = [asyncio.create_task(async_hello_world()) for i in range(3)]
    # for i in range(3):
    #     await async_hello_world()
    # await asyncio.sleep(4)
    print(conv_time(1743577202000))


# asyncio.run(main())

t = datetime.datetime.fromtimestamp(1743577202010/1000.0, pytz.timezone('Asia/Shanghai'))
print(t)
