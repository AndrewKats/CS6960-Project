from mpi4py import MPI

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

if rank == 0:
    data = [1, 2, 3, 4]
    comm.send(data, dest=1, tag=7)
    print 'Send', data
elif rank == 1:
    data = comm.recv(source=0, tag=7)
    print 'Received', data