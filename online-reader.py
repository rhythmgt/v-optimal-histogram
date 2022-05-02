import json
from time import sleep
from kafka import KafkaConsumer
from threading import Thread, Event
import random
import threading
import copy
import tkinter
import matplotlib
matplotlib.use('TkAgg')
import numpy as np
import matplotlib.pyplot as plt

## guha Algorithm
class bucketNode:
    def __init__(self, prevBucket):
        self.prev = prevBucket
        self.next = None
        if (prevBucket != None):
            prevBucket.next = self
        self.hist = histNode(None)
        self.lastNode = self.hist

class histNode:
    def __init__ (self, prevNode):
        self.prev = prevNode
        self.next = None
        self.a = 1
        self.b = 1
        self.sol = 0
        if (prevNode != None):
            prevNode.next = self

def error(a, b):
    s2 = cumSumSq[b] - cumSumSq[a-1]
    s1 = cumSum[b] - cumSum[a-1]
    return s2 - ((s1**2) / (b-a+1))


def initialisation(firstFreq):    
    originalHistogram[0] = firstFreq
    cumSum[0] = firstFreq
    cumSumSq[0] = firstFreq**2
    
    dpTable.hist.a = 0
    dpTable.hist.b = 0
    dpTable.hist.sol = error(0,0)
    currptr = dpTable
    currptr.lastNode = currptr.hist
    for i in range(num_buckets-1):
        bucketNode(currptr)
        currptr = currptr.next
        currptr.hist.a = 0
        currptr.hist.b = 0
        currptr.hist.sol = 0 #unsure
        currptr.lastNode = currptr.hist
    
    
    
def update(idx, freq):
    # update original histogram with freq
    originalHistogram[idx] = freq
    
    cumSum[idx] = freq + cumSum[idx-1]
    cumSumSq[idx] = freq**2 + cumSumSq[idx-1]
    #idx is 0 indexed
    
    currBucket = dpTable
    prevBucket = None
    ptr = None
    for b in range(num_buckets):
        currptr = currBucket.lastNode
        
        if (b==0):
            histNode(currptr)
            currptr = currptr.next
            currptr.a = idx
            currptr.b = idx
            minError = error(0, idx)
            currptr.sol = minError
            currBucket.lastNode = currptr
            
        else:
            minError = infinity
            if (idx < num_buckets-1):
                indices[b][idx] = idx
            for j in range(idx):
                        
                if not(ptr.a <= j<= ptr.b):
                    ptr = ptr.next
                    
                if (ptr.sol + error(j+1, idx) < minError):
                    minError = ptr.sol + error(j+1, idx)
                    indices[b][idx] = j

            if (minError > (1 + delta)*currptr.sol or currptr.sol == infinity):
                histNode(currptr)
                currptr = currptr.next
                currptr.sol = minError
                currptr.a = idx
                currptr.b = idx
                currBucket.lastNode = currptr

            else:
                currptr.b = idx
        prevBucket = currBucket
        ptr = prevBucket.hist
        currBucket = currBucket.next
        

def updateHistogram(idx):

    a = indices[num_buckets-1][idx]
    b = idx

    i = num_buckets-1
    while (i>0):
        vOptHistIndices[i] = b
        b = a

        if (i>1 and b >0):
            a = indices[i-1][b]
        i-=1
        if (b==0):
            break
    
    vOptHistIndices[i] = b
    i-=1
    while(i>=0):
        vOptHistIndices[i] = -1
        i-=1
    print("Optimal indices:", vOptHistIndices)

def query(idx, maxIdx):
    print("Query Initiated on index:", idx)
    gtValue = originalHistogram[idx]
    ptr = num_buckets-1
    a, b = vOptHistIndices[ptr-1] , vOptHistIndices[ptr]
    while(not(a+1<=idx<=b) and ptr>1):
        ptr-=1
        a, b = vOptHistIndices[ptr-1] , vOptHistIndices[ptr]
    if (not(a+1<=idx<=b)):
        if (vOptHistIndices[0]>-1 and idx<=vOptHistIndices[0]):
            a = -1
            b = vOptHistIndices[0]
        else:
            print("Error!")
            return
    predValue = (cumSum[b] - cumSum[a])/(b-a)
    return abs(gtValue - predValue)


def get_Predictions(maxIdx):
    predictedHistogram = {}
    ptr = num_buckets-1
    for idx in range(maxIdx, -1 , -1):
        a,b = vOptHistIndices[ptr-1] , vOptHistIndices[ptr]
        if (idx < maxIdx and (a+1<=idx<=b)):
            predictedHistogram[idx] = predictedHistogram[idx+1]
        else:
            if  (not(a+1<=idx<=b) and ptr>1):
                ptr-=1
                a,b = vOptHistIndices[ptr-1] , vOptHistIndices[ptr]
            
            if (not(a+1<=idx<=b)):
                if (vOptHistIndices[0]>-1 and idx<=vOptHistIndices[0]):
                    a = -1
                    b = vOptHistIndices[0]
                else:
                    print("Error while fetching predictions!")
                    return
            predictedHistogram[idx] = (cumSum[b] - cumSum[a])/(b-a)
    
    return copy.deepcopy(originalHistogram) , predictedHistogram


# global variables
originalHistogram = {}
delta = 2
num_buckets = 10
dpTable = bucketNode(None)
cumSum = {-1: 0}
cumSumSq = {-1 : 0}
indices = [{} for f in range(num_buckets)]

infinity = int(1e7)
vOptHistIndices = [0 for i in range(num_buckets)]
vOptHistValues = [0 for i in range(num_buckets)]

probability = 0.1
thresh = int(probability*100)

## end guha Algorithm


# counts number of messages in a time interval
def count_msg(var,consumer,lock):
    for message in consumer:
        lock.acquire()
        var[0]+=1
        lock.release()
        
 
def modify_variable(var, ind, lock):
    while True:
        sleep(2)
        lock.acquire()
        th = Thread(target = func, args =(var[0], ind ))
        th.start()
        var[0] = 0
        lock.release()

# calls guha algorithm on incoming data        
def func(count, ind):
    n = ind[0]
    if (ind[0]==0):
        initialisation(var[0])
    else:
        update(ind[0], var[0])
        updateHistogram(ind[0])      
        if(ind[0]>0 and ind[0]%10==0):
            act , pred = get_Predictions(ind[0])
            rows = ind[0]+1
            t1 = np.zeros(ind[0]+1)
            t2 = np.zeros(ind[0]+1)
            for i in range(ind[0]+1):
                t1[i] = act[i]
                t2[i] = pred[i]
            err = np.sum((t1-t2)**2)
            print("Total Squared Error for " + str(rows) + " rows:",  err)
            print("Mean Squared Error for " + str(rows) + " rows:",  err/rows)
            
            xaxis = np.linspace(1,rows,rows).astype("int64")
            xaxis2 = copy.deepcopy(xaxis)
            plt.figure(figsize=(20, 8))
            plt.plot( xaxis, t1,  "-b", label="Actual" )                                                   
            plt.plot( xaxis2, t2, "-r", label="Prediction")
            plt.legend()
            plt.xlabel("Time_Interval")
            plt.ylabel("Frequency_count")
            plt.title("Time_Interval-vs-Frequency_count || Buckets="+str(num_buckets)) 
            plt.savefig(str(rows)+'.png')
            #plt.show()
        # randomVal = random.randint(0, 100)
        # if randomVal<=thresh:
        #     randomIndx = random.randint(0, ind[0])
        #     print(query(randomIndx, ind[0]))
    ind[0]+=1
    


# receive data from producer and call above functions

dic = {}
ind = [0]
var = [0]
lock = threading.Lock()
consumer = KafkaConsumer ('testTopic',bootstrap_servers = ['localhost:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')),enable_auto_commit=True)
t = Thread(target=count_msg, args=(var, consumer, lock))
t.start()  
thread = Thread(target = modify_variable, args =(var, ind, lock))
thread.start()