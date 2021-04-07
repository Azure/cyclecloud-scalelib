import subprocess

def test_add_task():
    pass
    #with subprocess.Popen(["/opt/cycle/scalelib/venv/bin/python3"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True) as proc:
        

def setup_module():
    # start autoscale
    subprocess.run(['/opt/cycle/scalelib/venv/bin/python3', 'autoscale.py'])
    # wait for nodes
    out = subprocess.run(['/opt/cycle/scalelib/venv/bin/python3', '-m hpc.autoscale.cli', 'nodes'], stdout=subprocess.PIPE).stdout
    #python3 -m hpc.autoscale.cli