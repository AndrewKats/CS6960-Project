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
s1 = "shorter-s1.txt"
s2 = "shorter-s2.txt"
s3 = "shorter-s3.txt"
s4 = "shorter-s4.txt"
s5 = "shorter-s5.txt"
s6 = "shorter-s6.txt"
s7 = "shorter-s7.txt"
s8 = "shorter-s8.txt"

#c1 = "c1.txt"
#c2 = "c2.txt"
#c3 = "c3.txt"
#c4 = "c4.txt"
#c5 = "c5.txt"
#c6 = "c6.txt"
c1 = "shorter-c1.txt"
c2 = "shorter-c2.txt"
c3 = "shorter-c3.txt"
c4 = "shorter-c4.txt"
c5 = "shorter-c5.txt"
c6 = "shorter-c6.txt"
c7 = "shorter-c7.txt"
c8 = "shorter-c8.txt"

#p1 = "p1.txt"
#p2 = "p2.txt"
#p3 = "p3.txt"
#p4 = "p4.txt"
#p5 = "p5.txt"
#p6 = "p6.txt"
p1 = "shorter-p1.txt"
p2 = "shorter-p2.txt"
p3 = "shorter-p3.txt"
p4 = "shorter-p4.txt"
p5 = "shorter-p5.txt"
p6 = "shorter-p6.txt"
p7 = "shorter-p7.txt"
p8 = "shorter-p8.txt"

g1 = "shorter-g1.txt"
g2 = "shorter-g2.txt"
g3 = "shorter-g3.txt"
g4 = "shorter-g4.txt"
g5 = "shorter-g5.txt"
g6 = "shorter-g6.txt"
g7 = "shorter-g7.txt"
g8 = "shorter-g8.txt"


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


def reduceWords8(f1, f2, f3, f4, f5, f6, f7, f8):
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
    for key, value in f7.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    for key, value in f8.iteritems():
        if key not in wordDict:
            wordDict[key] = value
        else:
            wordDict[key] = wordDict[key] + value
    return wordDict

def reduceWords4(f1, f2, f3, f4):
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

def subtractReduce3(f1, f2, f3):
    wordDict = copy.deepcopy(f1)
    for key, value in f2.iteritems():
        wordDict[key] = wordDict[key] - value
    for key, value in f3.iteritems():
        wordDict[key] = wordDict[key] - value
    return wordDict






# N0
if rank == 0:
    # Needs S (S6, S7, S8)
    # Has S1, S2, S3, S4, S5, 
    # C1, C7, C8, 
    # P1, P2, P8, 
    # G1, G2, G3

    # Get word counts
    mapStart = time.time()
    S1 = mapWords(s1)
    S2 = mapWords(s2)
    S3 = mapWords(s3)
    S4 = mapWords(s4)
    S5 = mapWords(s5)
    C1 = mapWords(c1)
    C7 = mapWords(c7)
    C8 = mapWords(c8)
    P1 = mapWords(p1)
    P2 = mapWords(p2)
    P8 = mapWords(p8)
    G1 = mapWords(g1)
    G2 = mapWords(g2)
    G3 = mapWords(g3)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N0 mapped its words in ', mapTime

    rStart = time.time()
    C1xP2xG3 = reduceWords3(C1, P2, G3)
    print 'N0 created C1xP2xG3'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'C1xP2xG3 coded in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(C1xP2xG3, dest=1)
    print 'N0 sent C1xP2xG3 to N1', sys.getsizeof(C1xP2xG3)
    comm.send(C1xP2xG3, dest=2)
    print 'N0 sent C1xP2xG3 to N2', sys.getsizeof(C1xP2xG3)
    comm.send(C1xP2xG3, dest=5)
    print 'N0 sent C1xP2xG3 to N3', sys.getsizeof(C1xP2xG3)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N0 sent data to other nodes in', sTime

    # Receive
    S6xP1xG2 = comm.recv(source=1)
    print 'N0 received S6xP1xG2 from N1'
    S7xC8xG1 = comm.recv(source=2)
    print 'N0 received S7xC8xG1 from N2'
    S8xC7xP8 = comm.recv(source=5)
    print 'N0 received S8xC7xP8 from N3'

    rStart = time.time()
    S6 = subtractReduce3(S6xP1xG2, P1, G2)
    S7 = subtractReduce3(S7xC8xG1, C8, G1)
    S8 = subtractReduce3(S8xC7xP8, C7, P8)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S6, S7, S8 decoded in ' + str(rTime) + 'ms'


    start = time.time()
    allSWords = reduceWords8(S1, S2, S3, S4, S5, S6, S7, S8)
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
    # Needs C (C1, C7, C8)
    # Has S6, S7, S8, 
    # C2, C3, C4, C5, C6, 
    # P1, P2, P8, 
    # G1, G2, G3

    # Get word counts
    mapStart = time.time()
    S6 = mapWords(s6)
    S7 = mapWords(s7)
    S8 = mapWords(s8)
    C2 = mapWords(c2)
    C3 = mapWords(c3)
    C4 = mapWords(c4)
    C5 = mapWords(c5)
    C6 = mapWords(c6)
    P1 = mapWords(p1)
    P2 = mapWords(p2)
    P8 = mapWords(p8)
    G1 = mapWords(g1)
    G2 = mapWords(g2)
    G3 = mapWords(g3)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N1 mapped its words in ', mapTime

    rStart = time.time()
    S6xP1xG2 = reduceWords3(S6, P1, G2)
    print 'N1 created S6xP1xG2'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S6xP1xG2 coded in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(S6xP1xG2, dest=0)
    print 'N1 sent S6xP1xG2 to N0', sys.getsizeof(S6xP1xG2)
    comm.send(S6xP1xG2, dest=2)
    print 'N1 sent S6xP1xG2 to N2', sys.getsizeof(S6xP1xG2)
    comm.send(S6xP1xG2, dest=5)
    print 'N1 sent S6xP1xG2 to N3', sys.getsizeof(S6xP1xG2)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N1 sent data to other nodes in', sTime

    # Receive
    C1xP2xG3 = comm.recv(source=0)
    print 'N1 received C1xP2xG3 from N0'
    S7xC8xG1 = comm.recv(source=2)
    print 'N1 received S7xC8xG1 from N2'
    S8xC7xP8 = comm.recv(source=5)
    print 'N1 received S8xC7xP8 from N3'

    rStart = time.time()
    C1 = subtractReduce3(C1xP2xG3, P2, G3)
    C7 = subtractReduce3(S8xC7xP8, S8, P8)
    C8 = subtractReduce3(S7xC8xG1, S7, G1)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'C1, C7, C8 decoded in ' + str(rTime) + 'ms'

    start = time.time()
    allCWords = reduceWords8(C1, C2, C3, C4, C5, C6, C7, C8)
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
    # Needs P (P1, P2, P8)
    # Has S6, S7, S8 
    # C1, C7, C8 
    # P3, P4, P5, P6, P7
    # G1, G2, G3

    # Get word counts
    mapStart = time.time()
    S6 = mapWords(s6)
    S7 = mapWords(s7)
    S8 = mapWords(s8)
    C1 = mapWords(c1)
    C7 = mapWords(c7)
    C8 = mapWords(c8)
    P3 = mapWords(p3)
    P4 = mapWords(p4)
    P5 = mapWords(p5)
    P6 = mapWords(p6)
    P7 = mapWords(p7)
    G1 = mapWords(g1)
    G2 = mapWords(g2)
    G3 = mapWords(g3)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N2 mapped its words in ', mapTime

    rStart = time.time()
    S7xC8xG1 = reduceWords3(S7, C8, G1)
    print 'N2 created S7xC8xG1'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S7xC8xG1 coded in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(S7xC8xG1, dest=0)
    print 'N2 sent S7xC8xG1 to N0', sys.getsizeof(S7xC8xG1)
    comm.send(S7xC8xG1, dest=1)
    print 'N2 sent S7xC8xG1 to N1', sys.getsizeof(S7xC8xG1)
    comm.send(S7xC8xG1, dest=5)
    print 'N2 sent S7xC8xG1 to N5', sys.getsizeof(S7xC8xG1)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N2 sent data to other nodes in', sTime

    # Receive
    C1xP2xG3 = comm.recv(source=0)
    print 'N2 received C1xP2xG3 from N0'
    S6xP1xG2 = comm.recv(source=1)
    print 'N2 received S6xP1xG2 from N1'
    S8xC7xP8 = comm.recv(source=5)
    print 'N2 received S8xC7xP8 from N3'

    rStart = time.time()
    P1 = subtractReduce3(S6xP1xG2, S6, G2)
    P2 = subtractReduce3(C1xP2xG3, C1, G3)
    P8 = subtractReduce3(S8xC7xP8, C7, S8)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'P1, P2, P8 decoded in ' + str(rTime) + 'ms'

    start = time.time()
    allPWords = reduceWords8(P1, P2, P3, P4, P5, P6, P7, P8)
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
elif rank == 5:
    # Needs G (G1, G2, G3)
    # Has S6, S7, S8 
    # C1, C7, C8 
    # P1, P2, P8
    # G4, G5, G6, G7, G8

    # Get word counts
    mapStart = time.time()
    S6 = mapWords(s6)
    S7 = mapWords(s7)
    S8 = mapWords(s8)
    C1 = mapWords(c1)
    C7 = mapWords(c7)
    C8 = mapWords(c8)
    P1 = mapWords(p1)
    P2 = mapWords(p2)
    P8 = mapWords(p8)
    G4 = mapWords(g4)
    G5 = mapWords(g5)
    G6 = mapWords(g6)
    G7 = mapWords(g7)
    G8 = mapWords(g8)
    mapEnd = time.time()
    mapTime = 1000*(mapEnd - mapStart)
    print 'N3 mapped its words in ', mapTime

    rStart = time.time()
    S8xC7xP8 = reduceWords3(S8, C7, P8)
    print 'N3 created S8xC7xP8'
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'S8xC7xP8 coded in ' + str(rTime) + 'ms'

    # Send
    sStart = time.time()
    comm.send(S8xC7xP8, dest=0)
    print 'N3 sent S8xC7xP8 to N0', sys.getsizeof(S8xC7xP8)
    comm.send(S8xC7xP8, dest=1)
    print 'N3 sent S8xC7xP8 to N1', sys.getsizeof(S8xC7xP8)
    comm.send(S8xC7xP8, dest=2)
    print 'N3 sent S8xC7xP8 to N2', sys.getsizeof(S8xC7xP8)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N3 sent data to other nodes in', sTime

    # Receive
    C1xP2xG3 = comm.recv(source=0)
    print 'N3 received C1xP2xG3 from N0'
    S6xP1xG2 = comm.recv(source=1)
    print 'N3 received S6xP1xG2 from N1'
    S7xC8xG1 = comm.recv(source=2)
    print 'N3 received S7xC8xG1 from N3'

    rStart = time.time()
    G1 = subtractReduce3(S7xC8xG1, S7, C8)
    G2 = subtractReduce3(S6xP1xG2, S6, P1)
    G3 = subtractReduce3(C1xP2xG3, C1, P2)
    rEnd = time.time()
    rTime = 1000*(rEnd-rStart)
    print 'G1, G2, G3 decoded in ' + str(rTime) + 'ms'

    start = time.time()
    allGWords = reduceWords8(G1, G2, G3, G4, G5, G6, G7, G8)
    end = time.time()
    rtime = 1000*(end-start)
    print 'G reduced in ' + str(rtime) + 'ms'

    # Send to client
    sStart = time.time()
    comm.send(allGWords, dest=3)
    print 'N3 sent G to the client', sys.getsizeof(allGWords)
    sEnd = time.time()
    sTime = 1000*(sEnd - sStart)
    print 'N3 sent data to the client in', sTime

# CLIENT
elif rank == 3:
    start = time.time()

    data0 = comm.recv()
    print 'Client received 1'
    data1 = comm.recv()
    print 'Client received 2'
    data2 = comm.recv()
    print 'Client received 3'
    data3 = comm.recv()
    print 'Client received 4'

    rstart = time.time()
    allWords = reduceWords4(data0, data1, data2, data3)
    rend = time.time()
    rtime = 1000*(rend-rstart)
    print 'Client reduced all in ' + str(rtime) + 'ms'

    #print 'All words:', allWords

    end = time.time()
    donetime = 1000*(end-start)

    print 'DONE:', donetime

