from socket import *
import sys
import threading
import json
import random
from threading import *
import time
from queue import Queue
config=sys.argv[1]
algo=sys.argv[2]
f=open(config,"r")
d=dict()
data=json.loads(f.read())
w=data['workers']
global y
lock=Lock()
worker=None
prevRR=None
f1=open(algo,'a')
with open("logs.txt",'a') as f:
	f.write(algo+'\n')


def RR(work):
	global prevRR
	scheduledworker = -1
	selected = False
	end=-1
	j=0
	if prevRR==None:
		while (j < len(work)) and not selected :
				if work[j]['slots']>0:
					scheduledworker=j
					selected=True
				elif not selected:
					j+=1
	else:
		j=(prevRR+1)%len(work)
		end = prevRR
		while (j!=end) and not selected:
			if work[j]['slots']>0:
				scheduledworker= j
				selected=True
			j=(j+1)%len(work)

	if scheduledworker!=-1:
		prevRR = scheduledworker
	work[scheduledworker]['slots']-=1
	return work[scheduledworker]
		
		
	
def RANDOM(work):
	while(1):

		p=random.choice(work)
		#print(p['slots'])
		if(p['slots']==0):
			
			continue
		else:
			p['slots']-=1
			break
	return p
	
def LeastLoaded(work):
	while(1):
		j=0
		for i in range(1,len(work)):
			if work[j]['slots'] < work[i]['slots']:
				j= i
		    
		if work[j]['slots']<1:
			time.sleep(1)
		else:
			break
	work[j]['slots']-=1
	return work[j]
        
def initial_assign_job():
	if(algo=='RR'):
		y=RR(w)
	elif (algo=='LL'):
		y=LeastLoaded(w)
	elif(algo=='RANDOM'):
		y=RANDOM(w)
	else:
		sys.exit()
	
	return y

lock1=Lock()

def launch_task(x):
	
	#global t
	
	t=initial_assign_job()
	#x=q.get()
	print('task '+x['task_id']+' is assigned to worker_id '+str(t['worker_id'])+' '+str(t['slots']))
	z=socket(AF_INET,SOCK_STREAM)
	z.connect(("localhost",t['port']))
	msg2=json.dumps(x)
	z.send((msg2).encode())
	z.close()
	return 1
	
def request_for_job():
	#listen to job requests
	s=socket(AF_INET,SOCK_STREAM)
	s.bind(('localhost',5000))
	s.listen(1)
	
	while 1:
		lock.acquire()
		conn,addr=s.accept()
		x=conn.recv(1024)
		da=json.loads(x)
		d[da['job_id']]=len(da['map_tasks'])
		#joblist[int(da['job_id'])] = [str(time.time()),len(da['reduce_tasks'])]
		with open(algo,'a') as f:
			f.write(str(time.time())+','+'arrival time of job '+','+da['job_id']+','+str(len(da['reduce_tasks'])-1)+'\n')
		for i in range(len(da['map_tasks'])):
			#q.put(da['map_tasks'][i])
			launch_task(da['map_tasks'][i])
		while(d[da['job_id']]!=0):
			continue
		
		for i in range(len(da['reduce_tasks'])):
			#q.put(da['reduce_tasks'][i])
			launch_task(da['reduce_tasks'][i])
		
		#print(t['port'])

		
		lock.release()
	conn.close()

		
def update_from_worker(s1):
		
	
	
	while(1):
		#lock.acquire()
		conn2,add=s1.accept()
		x1=conn2.recv(1024)
		data=x1.decode().split('\n')
		msg1=json.loads(data[0])
		msg2=data[1]
		if(msg1['task_id'].split("_")[1][0]=='M'):
			d[msg1['task_id'].split("_")[0]]-=1
		  
		#print(msg1,msg2)
		k=0
		for i in w:
			if(i['worker_id']==int(msg2)):
				i['slots']+=1
				k=i['slots']
		
		print('task completed '+msg1['task_id']+' from worker '+msg2+' '+str(k))
		
		#lock.release()
	conn2.close()	
	
def main():
	#q=Queue()
	
	s1=socket(AF_INET,SOCK_STREAM)
	s1.bind(('localhost',5001))
	s1.listen(1)
	t1=threading.Thread(target=request_for_job)
	t2=threading.Thread(target=update_from_worker,args=(s1,))
	t1.start()
	t2.start()
	t1.join()
	t2.join()
if __name__=="__main__":
	main()

