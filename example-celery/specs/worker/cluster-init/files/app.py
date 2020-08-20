from celery import Celery

app = Celery('tasks', backend='rpc://', broker='pyamqp://guest@__localhost__//')

app.conf.update(
    task_serializer='json',
    accept_content=['json'], 
    result_serializer='json',
    timezone='America/Los_Angeles',
    enable_utc=True,
    worker_prefetch_multiplier=1
)

@app.task
def add(x, y, resttime=0):
    import time
    time.sleep(resttime)
    return x + y