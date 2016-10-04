import string, random
import numpy as np
import datetime

def randAN(stringsize=6):
    chars = string.lowercase + string.digits
    return ''.join((random.choice(chars)) for _ in xrange(stringsize))

def PowerLaw4(a=4.,m=1):
    #mean=1, power -4
    return np.floor(np.random.pareto(a)+1)*m

def time2real(s):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (s-epoch).total_seconds()

def Ftime(t, r):
    T=86400.
    return (1 + np.sin(2*np.pi*(t/T + r)))

def setPerUserModel():
    uid=randAN()
    rphase=random.random()
    weight=PowerLaw4()
    wordProb=np.abs(np.random.normal(2.5,1,2))
    return {'uid':uid, 'randomPhase': rphase, 'weight':weight,'wordProb': wordProb}

def setAllUserModel(NUser=10000,ofile='User'):
    import os
    ofile='/home/ubuntu/data/'+ofile+'NewModel.csv'
    with open(ofile, 'w') as um:
        for i in xrange(NUser):
            line=setPerUserModel()
            um.write('%s,'%line['uid'])
            um.write('%s,'%line['randomPhase'])
            um.write('%s,'%line['weight'])
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
        
def getRandomLine(afile):
    line=randomLineHelper(afile).split(',')
#    while random.random() > Ftime(t, float(line[1])):
#        line=randomLineHelper(afile).split(',')
    return line
    
def getMessage(t, afile='/home/ubuntu/data/UserNewModel.csv'):
    line=getRandomLine(afile)
    uid=line[0]
    tmp=Ftime(time2real(t), float(line[1]))
    weight=float(line[1])*tmp
    vecSize=111
    #print ind
    wordProb=[float(i.strip()) for i in line[-2:]]
    wordVec=np.random.beta(wordProb[0], wordProb[1], vecSize)
    #with open('data/topics.txt','r') as tpc:
    #    topics=[itm.strip() for itm in tpc.readlines()]
    #tags=[topics[ind[i]] for i,e in enumerate(wordProb) if e<random.random()]
    #return [uid, tags]
    return [uid, weight,]+list(wordVec)
            
#print 'test random ID: ', randAN()
#print 'test text message rate: ', PowerLaw4()
#print 'test time function: ', Ftime(1,.04)
