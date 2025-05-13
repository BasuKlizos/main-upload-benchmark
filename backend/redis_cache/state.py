import redis

r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)

def mark_file_complete(batch_id, filename):
    r.sadd(f"batch:{batch_id}:processed", filename)

def is_file_processed(batch_id, filename):
    return r.sismember(f"batch:{batch_id}:processed", filename)

def get_processed_count(batch_id):
    return r.scard(f"batch:{batch_id}:processed")
