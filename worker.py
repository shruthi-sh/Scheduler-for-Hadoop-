from socket import *
import sys
import threading
import json
from queue import Queue 
import time
from threading import *
port=sys.argv[1]
w_id=sys.argv[2]

lock=Lock()
#Listen to Master and receive task, note arrival time in logs, launch a thread to execute it. 
def task_from_master(q):
	s=socket(AF_INET,SOCK_STREAM)
	s.bind(('localhost',int(port)))
	s.listen(1)
	while 1:
		#lock.acquire()
		conn,addr=s.accept()
		x=conn.recv(1024)
		da=x.decode()
		
		task=json.loads(da)
		with open("logs.txt",'a') as f:
			f.write(task['task_id']+','+'arrival time '+','+str(time.time())+','+'from worker_id'+','+w_id+'\n')
		#f.close()
		q.put(task)
		t2=threading.Thread(target=execute_task,args=(q,))
		t2.start()
		t2.join()
			
		
		#lock.release()
	#conn.close()

#Log end time of task and send update to Master
def task_update_master(task):
		s=socket(AF_INET,SOCK_STREAM)
		lock.acquire()
		#f.write(str(time.time())+'->'+'Arrival time '+task['task_id'])
		with open("logs.txt",'a') as f:
			f.write(task['task_id']+','+'end time'+','+str(time.time())+','+'from worker_id'+','+w_id+'\n')
		print('execution of\t'+task['task_id']+'\tcompleted')
		s.connect(("localhost", 5001))
		message=json.dumps(task)+'\n'+w_id
		s.send(message.encode())
		lock.release()
		s.close()
		f.close()
	
#simulate execution by sleeping for task duration	
def execute_task(q):
	
	if q.empty():
		time.sleep(1)
	else:
		while not q.empty():
			task=q.get()
			#while(task['duration']!=0):
			#	task['duration']-=1
			time.sleep(task['duration'])
			task['duration']=0

			task_update_master(task)
	
	
	


def main():
	t=Queue()
	
	t1=threading.Thread(target=task_from_master,args=(t,))
	
	
	
	t1.start()
	#t2.start()
	t1.join()
	#t2.join()
if __name__=="__main__":
	main()


