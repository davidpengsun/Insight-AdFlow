import datetime
import string, random
import numpy as np

def randAN(stringsize=6):
    chars = string.lowercase + string.digits
    return ''.join((random.choice(chars)) for _ in xrange(stringsize))

def PowerLaw4(a=4.,m=1):
    #mean=1, power -4
    return np.floor(np.random.pareto(a)+1)*m

def Ftime(t, r):
    T=86400.
    return .5 + .5*np.sin(2*np.pi*(t/T + r))

def setPerUserModel():
    uid=randAN()
    rphase=random.random()
    wordProb=np.random.uniform(0,1,111)
    return {'uid':uid, 'randomPhase': rphase, 'wordProb': wordProb}

def setAllUserModel(NUser=10000):
    import os
    with open('/home/ubuntu/data/UserModel.csv', 'w') as um:
        for i in xrange(NUser):
            line=setPerUserModel()
            um.write('%s,'%line['uid'])
            um.write('%s,'%line['randomPhase'])
            for itm in line['wordProb']:
                um.write('%s,'%itm)
            um.seek(-1, os.SEEK_CUR)
            um.write("\n")

def randomLineHelper(afile):
    afile=open(afile,'r')
    line = next(afile)
    for num, aline in enumerate(afile):
      if random.randrange(num + 2): continue
      line = aline
    afile.close()
    return line
        
def getRandomLine(t, afile):
    line=randomLineHelper(afile).split(',')
    while random.random() > Ftime(getTimeInSec(t), np.float(line[1])):
        line=randomLineHelper(afile).split(',')
    return line
   
def getTimeInSec(t):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (t-epoch).total_seconds()
 
def getMessage(t, afile='/home/ubuntu/data/UserModel.csv', nsample=10):
    line=getRandomLine(t, afile)
    uid=line[0]
    ind=random.sample(range(111),nsample)
    #print ind
    wordProb=[float(line[2+i].strip()) for i in ind]
    #print wordProb
    with open('/home/ubuntu/data/topics.txt','r') as tpc:
        topics=[itm.strip() for itm in tpc.readlines()]
    tags=[topics[ind[i]] for i,e in enumerate(wordProb) if e<random.random()]
    return [uid, tags] 
if __name__ == '__main__':
    print 'test random ID: ', randAN()
    print 'test text message rate: ', PowerLaw4()
    print 'test time function: ', Ftime(1,.04)
    s=datetime.datetime.utcnow()
    print s.isoformat()
    print [s.isoformat()] + getMessage(s)
