import requests
from time import time

# import redis

from config import REDIS_URL, LINE_TOKEN

# kvs = redis.from_url('redis://@localhost:6379/0', decode_responses=True)
# kvs.set('is_busy', 0)


class Takenoko:
    # tqdmみたいなイテレータ。要はプログレスバー
    # タケノコに見えないのはご愛嬌
    def __init__(self, list, times=10):
        self.start = time()
        self.list = list
        self.current = 0 # 処理中のインデックス
        self.grow = 0 # タケノコの成長具合
        self.length = len(list)
        if self.length < times:
            times = self.length
        self.times = times
        # リスト全体を等分し、途中通過地点を格納したリストを作成
        interval = self.length / times
        self.process = [int(i * interval)-1 for i in range(1,times+1)]

    # まず__iter__メソッドが呼ばれてから__next__が呼ばれる
    def __iter__(self):
        return self

    def __next__(self):
        index = self.current
        max = self.length
        if index >= max:
            elapsed = time() - self.start
            print(f'>>> Done!  length: {max}  minutes: {round(elapsed/60)}')
            raise StopIteration()
        elif index >= self.process[self.grow]:
            self.grow += 1
            elapsed = time() - self.start
            msg = f'>{"=" * self.grow + "." * (self.times - self.grow)}< {index + 1} / {max}  {round(elapsed/60)}m'
            print(msg)
        self.current += 1
        return self.list[index]

# class Status:
#     def update_status(self, status):
#         kvs.set('is_busy', status)
#         print(f'status updated. is_busy: {status}')
#
#     def free(self):
#         self.update_status('free')
#
#     def busy(self):
#         self.update_status('busy')
#
#     def get_status(self):
#         with open(self.path, 'r') as f:
#             status = f.read()
#         if status not in ['busy', 'free']:
#             raise Exception('ステータスファイルの値が無効です！')
#         return status


def notify_line(message):
    url = "https://notify-api.line.me/api/notify"
    print(message)
    if LINE_TOKEN:
        headers = {'Authorization': 'Bearer ' + LINE_TOKEN}
        payload = {'message': message, 'notificationDisabled': True}
        r = requests.post(url, headers=headers, params=payload)
