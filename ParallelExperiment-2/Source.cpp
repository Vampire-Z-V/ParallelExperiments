# include <stdio.h>
# include <stdlib.h>
# include <assert.h>
# include <time.h>
#include <mpi.h>

#include "PSRS_MPI.h"

#define BLOCK 1024*1024

int main(int argc, char *argv[])
{
	if (argc != 3)
	{
		printf("Usage:%s loop_times data_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	// ≥ı ºªØMPI
	int size, rank;
	MPI_Init(nullptr, nullptr);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	printf("process: %d of %d\n", rank, size);

	int loop = atoi(argv[1]);
	int number_total_length = BLOCK * loop;

	int *numbers = PSRS(argv[2], number_total_length, rank, size);

	MPI_Finalize();

	delete[] numbers;
	return 0;
}