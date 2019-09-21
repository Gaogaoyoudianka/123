#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time, datetime 
SHADOW_CLIENT="A0195045A_Gao_thing2"
HOST_NAME="a14f8ks99wxpvb-ats.iot.us-west-2.amazonaws.com"
ROOT_CA="AmazonRootCA1_thing2.pem"
PRIVATE_KEY="1fb33f5e73-private.pem.key"
CERT_FILE="1fb33f5e73-certificate.pem.crt.txt"
SHADOW_HANDLER="A0195045A_Gao_thing2"



def myShadowUpdateCallback(payload,responseStatus,token):
    print()
    print('UPDATE:$aws/things/'+SHADOW_HANDLER+
          '/shadow/update/#')
   


myShadowClient=AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME,8883)
myShadowClient.configureCredentials(ROOT_CA,PRIVATE_KEY,CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()



myDeviceShadow=myShadowClient.createShadowHandlerWithName(SHADOW_HANDLER,True)


#main script
infile = open('train_FD002.txt','r')
outfile = open('train_FD002','a')


sensor = ['s'+ str(i) for i in range(1,22)]
labels = ['id', 'timestamp', 'Matricnumber', 'te', 'os1', 'os2', 'os3'] + sensor
Matricnumber = 'A0195045A'

for i in range(0,len(labels)):
    labels[i] = '\"' + labels[i] + '\"'
    

for line in infile.readlines():
    outfile.write(line)
    
process = open("train_FD002", 'r')

datastring = []
modified = []

head = '{"state":{"reported":{'
tail= '}}}'

#read data, add additional texts and send out messages
for x in process.readlines():
    newdata = x.split(" ")
    modified = []
    modified.append(str('FD002_' + newdata[0]))
    modified.append(str(datetime.datetime.utcnow()))
    modified.append(Matricnumber)
    for j in range(2,len(sensor)):
        modified.append(newdata[j])
        
    colabels = []
    colabels.append(str(labels[0] + ':'))
    colabels.append(str('"' + modified[0] + '",'))
    colabels.append(str(labels[1] + ':'))
    colabels.append(str('"' + str(datetime.datetime.now()) + '",'))
    colabels.append(str(labels[2] + ':'))
    colabels.append(str('"' + Matricnumber + '",'))
    
    for i in range(3,len(labels)):
        colabels.append(str(labels[i] + ':'))  
        colabels.append(str('"' + newdata[i-2] + '",'))
        
    string = ''.join(colabels)
    string = string[:-1]
    
    data = []
    data.append(head)
    data.append(string)
    data.append(tail)
    data.append('\n')
    datastring = ''.join(data)
    print(datastring)
    
    myDeviceShadow.shadowUpdate(datastring, myShadowUpdateCallback, 5)
    
    time.sleep(0.1)
    

