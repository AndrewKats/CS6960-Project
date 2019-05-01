from mpi4py import MPI
import time
import copy
import sys

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

#s1 = "s1.txt"
#s2 = "s2.txt"
#s3 = "s3.txt"
#s4 = "s4.txt"
#s5 = "s5.txt"
#s6 = "s6.txt"
s1 = "short-s1.txt"
s2 = "short-s2.txt"
s3 = "short-s3.txt"
s4 = "short-s4.txt"
s5 = "short-s5.txt"
s6 = "short-s6.txt"

#c1 = "c1.txt"
#c2 = "c2.txt"
#c3 = "c3.txt"
#c4 = "c4.txt"
#c5 = "c5.txt"
#c6 = "c6.txt"
c1 = "short-c1.txt"
c2 = "short-c2.txt"
c3 = "short-c3.txt"
c4 = "short-c4.txt"
c5 = "short-c5.txt"
c6 = "short-c6.txt"

#p1 = "p1.txt"
#p2 = "p2.txt"
#p3 = "p3.txt"
#p4 = "p4.txt"
#p5 = "p5.txt"
#p6 = "p6.txt"
p1 = "short-p1.txt"
p2 = "short-p2.txt"
p3 = "short-p3.txt"
p4 = "short-p4.txt"
p5 = "short-p5.txt"
p6 = "short-p6.txt"


def mapWords(filename):
    start = time.time()

    file = open(filename)

    wordDict = {}
    for line in file:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1

    end = time.time()
    maptime = 1000 * (end-start)
    #print filename + ' mapped in ' + str(maptime) + ' ms'

    return wordDict


def reduceWords6(f1, f2, f3, f4, f5, f6):
    wordDict = copy.deepcopy(f1)
    for key, value in f2.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f3.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f4.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f5.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f6.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    return wordDict


def reduceWords3(f1, f2, f3):
    wordDict = copy.deepcopy(f1)
    for key, value in f2.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f3.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    return wordDict

def reduceWords2(f1, f2):
    wordDict = copy.deepcopy(f1)
    for key, value in f2.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    return wordDict

def subtractReduce(f1, f2):
    wordDict = copy.deepcopy(f1)
    for key, value in f2.iteritems():
        wordDict[key] = wordDict[key] - value
    return wordDict

def countAllWords():
    wordDict = {}

    sfile1 = open(s1)
    for line in sfile1:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile1.close()

    sfile2 = open(s2)
    for line in sfile2:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile2.close()

    sfile3 = open(s3)
    for line in sfile3:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile3.close()

    sfile4 = open(s4)
    for line in sfile4:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile4.close()

    sfile5 = open(s5)
    for line in sfile5:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile5.close()

    sfile6 = open(s6)
    for line in sfile6:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile6.close()



    cfile1 = open(c1)
    for line in cfile1:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile1.close()

    cfile2 = open(c2)
    for line in cfile2:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile2.close()

    cfile3 = open(c3)
    for line in cfile3:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile3.close()

    cfile4 = open(s4)
    for line in cfile4:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile4.close()

    cfile5 = open(s5)
    for line in cfile5:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile5.close()

    cfile6 = open(c6)
    for line in cfile6:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile6.close()


    pfile1 = open(p1)
    for line in pfile1:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile1.close()

    pfile2 = open(p2)
    for line in pfile2:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile2.close()

    pfile3 = open(p3)
    for line in pfile3:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile3.close()

    pfile4 = open(p4)
    for line in pfile4:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile4.close()

    pfile5 = open(p5)
    for line in pfile5:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile5.close()

    pfile6 = open(p6)
    for line in pfile6:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile6.close()

    return wordDict

# N0
if rank == 0:
    # Needs S (S5, S6)
    # Has S1, S2, S3, S4, C1, C2, P3, P4

    # Get word counts
    mapStart = time.time()
    S1 = mapWords(s1)
    S2 = mapWords(s2)
    S3 = mapWords(s3)
    S4 = mapWords(s4)
    C1 = mapWords(c1)
    C2 = mapWords(c2)
    P3 = mapWords(p3)
    P4 = mapWords(p4)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N0 mapped its words in ', mapTime

    rStart = time.time()
    C1xP3 = reduceWords2(C1, P3)
    print 'N0 created C1xP3'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'C1xP3 reduced in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(C1xP3, dest=1)
    print 'N0 sent C1xP3 to N1', sys.getsizeof(C1xP3)
    comm.send(C1xP3, dest=2)
    print 'N0 sent C1xP3 to N2', sys.getsizeof(C1xP3)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N0 sent data to other nodes in', sTime

    # Receive
    P4xS5 = comm.recv(source=1)
    print 'N0 received P4xS5 from N1'
    S6xC2 = comm.recv(source=2)
    print 'N0 received S6xC2 from N2'

    rStart = time.time()
    S5 = subtractReduce(P4xS5, P4)
    S6 = subtractReduce(S6xC2, C2)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S5 and S6 reduced in ' + str(rTime) + 'ms'


    start = time.time()
    allSWords = reduceWords6(S1, S2, S3, S4, S5, S6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'S reduced in ' + str(rtime) + 'ms'

    # Send to client
    sStart = time.time()
    comm.send(allSWords, dest=3)
    print 'N0 sent S to the client', sys.getsizeof(allSWords)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N0 sent data to the client in', sTime

# N1
elif rank == 1:
    # Needs C (C1, C2)
    # Has S5, S6, C3, C4, C5, C6, P3, P4

    # Get word counts
    mapStart = time.time()
    S5 = mapWords(s5)
    S6 = mapWords(s6)
    C3 = mapWords(c3)
    C4 = mapWords(c4)
    C5 = mapWords(c5)
    C6 = mapWords(c6)
    P3 = mapWords(p3)
    P4 = mapWords(p4)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N1 mapped its words in ', mapTime

    rStart = time.time()
    P4xS5 = reduceWords2(P4, S5)
    print 'N1 created P4xS5'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'P4xS5 reduced in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(P4xS5, dest=0)
    print 'N1 sent P4xS5 to N0', sys.getsizeof(P4xS5)
    comm.send(P4xS5, dest=2)
    print 'N1 sent P4xS5 to N2', sys.getsizeof(P4xS5)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N1 sent data to other nodes in', sTime

    # Receive
    C1xP3 = comm.recv(source=0)
    print 'N1 received C1xP3 from N0'
    S6xC2 = comm.recv(source=2)
    print 'N1 received S6xC2 from N2'

    rStart = time.time()
    C1 = subtractReduce(C1xP3, P3)
    C2 = subtractReduce(S6xC2, S6)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S5 and S6 reduced in ' + str(rTime) + 'ms'

    start = time.time()
    allCWords = reduceWords6(C1, C2, C3, C4, C5, C6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'C reduced in ' + str(rtime) +'ms'

    # Send to client
    sStart = time.time()
    comm.send(allCWords, dest=3)
    print 'N1 sent C to the client', sys.getsizeof(allCWords)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N1 sent data to the client in', sTime

# N2
elif rank == 2:
    # Needs P (P3, P4)
    # Has S5, S6, C1, C2, P1, P2, P5, P6

    # Get word counts
    mapStart = time.time()
    S5 = mapWords(s5)
    S6 = mapWords(s6)
    C1 = mapWords(c1)
    C2 = mapWords(c2)
    P1 = mapWords(p1)
    P2 = mapWords(p2)
    P5 = mapWords(p5)
    P6 = mapWords(p6)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N2 mapped its words in ', mapTime

    rStart = time.time()
    S6xC2 = reduceWords2(S6, C2)
    print 'N2 created S6xC2'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'P4xS5 reduced in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(S6xC2, dest=0)
    print 'N2 sent S6xC2 to N0', sys.getsizeof(S6xC2)
    comm.send(S6xC2, dest=1)
    print 'N2 sent S6xC2 to N1', sys.getsizeof(S6xC2)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N2 sent data to other nodes in', sTime

    # Receive
    C1xP3 = comm.recv(source=0)
    print 'N2 received C1xP3 from N0'
    P4xS5 = comm.recv(source=1)
    print 'N2 received P4xS5 from N1'

    rStart = time.time()
    P3 = subtractReduce(C1xP3, C1)
    P4 = subtractReduce(P4xS5, S5)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S5 and S6 reduced in ' + str(rTime) + 'ms'

    start = time.time()
    allPWords = reduceWords6(P1, P2, P3, P4, P5, P6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'P reduced in ' + str(rtime) + 'ms'

    # Send to client
    sStart = time.time()
    comm.send(allPWords, dest=3)
    print 'N2 sent P to the client', sys.getsizeof(allPWords)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N2 sent data to the client in', sTime

# N3
elif rank == 4:
    rStart = time.time()
    allWords = countAllWords()
    rEnd = time.time()
    rTime = 1000*(rEnd - rStart)
    print 'N3 counted all words in', rTime

    sStart = time.time()
    comm.send(allWords, dest=3)
    print 'N3 sent S+C+P to the client', sys.getsizeof(allWords)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N3 sent data to the client in', sTime

# CLIENT
elif rank == 3:
    start = time.time()

    data0 = comm.recv()
    #print 'Client received', data0
    data1 = comm.recv()
    #print 'Client received', data1
    data2 = comm.recv()
    #print 'Client received', data2

    rstart = time.time()
    allWords = reduceWords3(data0, data1, data2)
    rend = time.time()
    rtime = 1000*(rend-rstart)
    print 'Client reduced all in ' + str(rtime) + 'ms'

    #print 'All words:', allWords

    end = time.time()
    donetime = 1000*(end-start)

    print 'DONE:', donetime

