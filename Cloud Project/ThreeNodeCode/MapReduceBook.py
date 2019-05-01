from mpi4py import MPI
import time
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
    wordDict = f1
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
    wordDict = f1
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



# N0
if rank == 0:
    # Needs S (S3, S4, S5, S6)
    # Has S1, S2, C1, C2, P1, P2

    # Get word counts
    mapStart = time.time() 
    S1 = mapWords(s1)
    S2 = mapWords(s2)
    C1 = mapWords(c1)
    C2 = mapWords(c2)
    P1 = mapWords(p1)
    P2 = mapWords(p2)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N0 mapped its words in ', mapTime

    # Send
    sStart = time.time()
    comm.send(C1, dest=1)
    print 'N0 sent C1 to N1', sys.getsizeof(C1)
    comm.send(C2, dest=1)
    print 'N0 sent C2 to N1', sys.getsizeof(C2)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N0 sent data to N1 in', sTime

    sStart = time.time()
    comm.send(P1, dest=2)
    print 'N0 sent P1 to N2', sys.getsizeof(P1)
    comm.send(P2, dest=2)
    print 'N0 sent P2 TO N2', sys.getsizeof(P2)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N0 sent data to N2 in', sTime

    # Receive
    S3 = comm.recv(source=1)
    print 'N0 received S3 from N1'
    S4 = comm.recv(source=1)
    print 'N0 received S4 from N1'

    S5 = comm.recv(source=2)
    print 'N0 received S5 from N2'
    S6 = comm.recv(source=2)
    print 'N0 received S6 from N2'

    start = time.time()
    allSWords = reduceWords6(S1, S2, S3, S4, S5, S6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'S reduced in ' + str(rtime) + 'ms'

    # Send to client
    comm.send(allSWords, dest=3)
    print 'N0 sent S to the client', sys.getsizeof(allSWords)

# N1
elif rank == 1:
    # Needs C (C1, C2, C5, C6)
    # Has S3, S4, C3, C4, P3, P4

    # Get word counts
    mapStart = time.time()
    S3 = mapWords(s3)
    S4 = mapWords(s4)
    C3 = mapWords(c3)
    C4 = mapWords(c4)
    P3 = mapWords(p3)
    P4 = mapWords(p4)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N1 mapped its words in ', mapTime

    # Send
    sStart = time.time()
    comm.send(S3, dest=0)
    print 'N1 sent S3 to N0', sys.getsizeof(S3)
    comm.send(S4, dest=0)
    print 'N1 sent S4 to N0', sys.getsizeof(S4)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N1 sent data to N0 in', sTime

    sStart = time.time()
    comm.send(P3, dest=2)
    print 'N1 sent P3 to N2', sys.getsizeof(P3)
    comm.send(P4, dest=2)
    print 'N1 sent P4 to N2', sys.getsizeof(P4)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N1 sent data to N2 in', sTime

    # Receive
    C1 = comm.recv(source=0)
    print 'N1 received C1 from N0'
    C2 = comm.recv(source=0)
    print 'N1 received C2 from N0'

    C5 = comm.recv(source=2)
    print 'N1 received C5 from N2'
    C6 = comm.recv(source=2)
    print 'N1 received C6 from N2'

    start = time.time()
    allCWords = reduceWords6(C1, C2, C3, C4, C5, C6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'C reduced in ' + str(rtime) +'ms'

    # Send to client
    comm.send(allCWords, dest=3)
    print 'N1 sent C to the client', sys.getsizeof(allCWords)

# N2
elif rank == 2:
    # Needs P (P1, P2, P3, P4)
    # Has S5, S6, C5, C6, P5, P6

    # Get word counts
    mapStart = time.time()
    S5 = mapWords(s5)
    S6 = mapWords(s6)
    C5 = mapWords(c5)
    C6 = mapWords(c6)
    P5 = mapWords(p5)
    P6 = mapWords(p6)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N2 mapped its words in ', mapTime

    # Send
    sStart = time.time()
    comm.send(S5, dest=0)
    print 'N2 sent S5 to N0', sys.getsizeof(S5)
    comm.send(S6, dest=0)
    print 'N2 sent S6 to N0', sys.getsizeof(S6)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N2 sent data to N1 in', sTime

    sStart = time.time()
    comm.send(C5, dest=1)
    print 'N2 sent C5 to N1', sys.getsizeof(C5)
    comm.send(C6, dest=1)
    print 'N2 sent C6 to N1', sys.getsizeof(C6)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N2 sent data to N1 in', sTime

    # Receive
    P1 = comm.recv(source=0)
    print 'N2 received P1 from N0'
    P2 = comm.recv(source=0)
    print 'N2 received P2 from N0'

    P3 = comm.recv(source=1)
    print 'N2 received P3 from N1'
    P4 = comm.recv(source=1)
    print 'N2 received P4 from N1'

    start = time.time()
    allPWords = reduceWords6(P1, P2, P3, P4, P5, P6)
    end = time.time()
    rtime = 1000*(end-start)
    print 'P reduced in ' + str(rtime) + 'ms'

    # Send to client
    comm.send(allPWords, dest=3)
    print 'N2 sent P to the client', sys.getsizeof(allPWords)

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
    print 'All reduced in ' + str(rtime) + 'ms'

    #print 'All words:', allWords

    end = time.time()
    donetime = 1000*(end-start)

    print 'DONE:', donetime

