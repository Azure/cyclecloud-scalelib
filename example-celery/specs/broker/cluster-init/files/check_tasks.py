from celery import Celery
app = Celery()
appc = app.control.inspect()
print("Running Tasks: \n %s" % appc.active())
print("Pending Tasks: \n %s" % appc.reserved())