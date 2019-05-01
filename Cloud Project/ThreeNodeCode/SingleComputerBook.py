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



start = time.time()
countAllWords()
end = time.time()
runtime = 1000*(end-start)
print "Counted all in", runtime