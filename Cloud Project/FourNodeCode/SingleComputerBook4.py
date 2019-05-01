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

    sfile7 = open(s7)
    for line in sfile7:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile7.close()

    sfile8 = open(s8)
    for line in sfile8:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    sfile8.close()

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

    cfile7 = open(c7)
    for line in cfile7:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile7.close()

    cfile8 = open(c8)
    for line in cfile8:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    cfile8.close()


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

    pfile7 = open(p7)
    for line in pfile7:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile7.close()

    pfile8 = open(p8)
    for line in pfile8:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    pfile8.close()



    gfile1 = open(g1)
    for line in gfile1:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile1.close()

    gfile2 = open(g2)
    for line in gfile2:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile2.close()

    gfile3 = open(g3)
    for line in gfile3:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile3.close()

    gfile4 = open(g4)
    for line in gfile4:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile4.close()

    gfile5 = open(g5)
    for line in gfile5:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile5.close()

    gfile6 = open(g6)
    for line in gfile6:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile6.close()

    gfile7 = open(g7)
    for line in gfile7:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile7.close()

    gfile8 = open(g8)
    for line in gfile8:
        stripped = line.strip()
        words = stripped.split()
        for word in words:
            if word not in wordDict:
                wordDict[word] = 1
            else:
                wordDict[word] = wordDict[word] + 1
    gfile8.close()

    return wordDict



start = time.time()
countAllWords()
end = time.time()
runtime = 1000*(end-start)
print "Counted all in", runtime