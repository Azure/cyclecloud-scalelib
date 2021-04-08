import subprocess

def add_task_and_check():
    subprocess.check_output("/opt/cycle/scalelib/venv/bin/python3 add_task.py", shell=True)
    out = subprocess.check_output("/opt/cycle/scalelib/venv/bin/python3 check_tasks.py", shell=True).decode('utf-8')
    tasks = [line for line in out.split("\n") if "Running Tasks:" not in line and "Pending Tasks:" not in line]
    for task in tasks:
        yield task

# No worker started, so this will fail (at least the first time it's run)
def test_fail():
    for task in add_task_and_check():
        assert "None" in task

# Worker started, so this will pass
def test_add_task():
    for task in add_task_and_check():
        assert "None" not in task
        

def setup_module():
    # start celery worker
    subprocess.check_output("/opt/cycle/scalelib/venv/bin/python3 -m celery -A tasks worker -D", shell=True)
    # start autoscale
    subprocess.run(['/opt/cycle/scalelib/venv/bin/python3', 'autoscale.py'])
    # wait for a node to boot up
    out = subprocess.check_output("/opt/cycle/scalelib/venv/bin/python3 -m hpc.autoscale.cli nodes", shell=True).decode('utf-8')
    '''
    while True:
        out = subprocess.check_output("/opt/cycle/scalelib/venv/bin/python3 -m hpc.autoscale.cli nodes", shell=True).decode('utf-8')
        workers = [line for line in out.split("\n") if line[:7] == "worker-"]
        for worker in workers:
            if "Ready" in worker:
                return
    '''