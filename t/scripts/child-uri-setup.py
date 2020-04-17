import time
import sys
import flux
import socket

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

try:
	parent_handle = flux.Flux(url=parent_uri)
	if not parent_uri.decode("utf-8").endswith("0/local"):
		hostname = socket.gethostname()
		attr_val = my_uri.decode("utf-8").replace("local:///", 
								 "ssh://" + hostname + "/")

	else:
		attr_val = my_uri

except Exception as e:
	print(e)
	sys.exit(1)

success = parent_handle.attr_set("child-uri" + "-" + str(jobid), attr_val)

# sleep is for testing only
time.sleep(600)
