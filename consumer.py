from kafka import KafkaConsumer
import pandas as pd
import pandas as pd
from collections import defaultdict
import random
import datetime
import re
import matplotlib.pyplot as plt
def comp_x_val(list1):

    xval = []
    for i in range(len(list1)):
        count = 0;
        for j in range(len(list1) - i):
            if (list1[i] == list1[i + j]):
                count += 1
        xval.append(count)

    return xval
def surprise(freq_list):
    Xvalue = comp_x_val(freq_list)
    #print(Xvalue)
    surp_no=0
    for i in range(len(Xvalue)):
        surp_no=surp_no+ (Xvalue[i]**2)
    return surp_no



def func(stop_t):

    # set-up a Kafka consumer
    count=0
    list_s=[]
    consumer = KafkaConsumer('covid')
    t1=datetime.datetime.now()
    surp_list=[]
    initial={}
    size=10000

    for msg in consumer:
        id=(msg.value).decode('utf-8')
        print(id)
        if(len(initial)<size):

            if id in initial:
                x=initial[id]
                initial[id]=x+1
            else:
                initial[id]=1
        else:
            x=random.random()*(len(initial))
            if int(x)<=size:
                list0 = [(k, v) for k, v in initial.items()]
                list0[int(x)]=(id,1)
                initial=dict(list0)


        t2=datetime.datetime.now()
        if int((t2-t1).total_seconds()/60)>9+count*stop_t and int((t2-t1).total_seconds()/60)%stop_t==0:
            count=count+1
            print(initial)
            list_s.append(surprise(list(initial.values())))
            if count==15:
                break


    return list_s





if __name__ == "__main__":
    l=[10,20,30,40]
    fin=[]
    for i in l:
        l1=func(i)
        fin.append(l1)
    print(fin)
    s=range(1,16)
    p=[]
    for j in fin:
        a=j[0]
        b=j[1]
        d=sum(b)/len(b)
        p.append(d)
        plt.plot(s, b, label = str(a)+"mins")

# naming the x axis
plt.xlabel('trial')
# naming the y axis
plt.ylabel('surprise number')

plt.legend()

# function to show the plot
plt.show()
print(p)
