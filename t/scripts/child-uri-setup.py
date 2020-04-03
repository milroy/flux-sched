import time
import sys
import flux

jobid = 0
f = flux.Flux()
my_uri = f.attr_get("local-uri")
try:
	jobid = f.attr_get("jobid")
except:
	pass

try:
	parent_uri = f.attr_get("parent-uri")
except Exception as e:
	print(e)
	sys.exit(1)
	
parent_handle = flux.Flux(url=parent_uri)
success = parent_handle.attr_set("child-uri" + "-" + str(jobid), my_uri)
# sleep is for testing only
time.sleep(600)
