import redis

# r = redis.Redis(host='localhost')
# redis_pool = redis.ConnectionPool(host='localhost', decode_responses=True)
# r = redis.StrictRedis(connection_pool=redis_pool)
# r = redis.from_url(os.environ.get("REDIS_URL"))
r = redis.from_url('redis://@localhost:6379/0', decode_responses=True)
# Keyを"key"にしてvalue"foo"を登録
r.set("key", "ぬいは")
print(r.get("key"))

# Keyを"test_int"にしてvalue 10を登録
r.set("test_int",10)

# インクリメントできる
r.incr("test_int")
print(r.get("test_int"))
#=>b'11'

# 削除
r.flushall()
