#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include <string>
#include <sstream>
using namespace std;

//#define ARRAY_LENGTH(process_count) (6400 / process_count)
//#define BOUNDARY(round, process_count) (6400 / process_count * (round - 1))

#define ARRAY_LENGTH(process_count) ((1LL << 32) / process_count)
#define BOUNDARY(round, process_count) ((1LL << 32) / process_count * (round - 1))


void DisplayArray(long long *_array, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%lld ", _array[i]);
	}
	printf("\n");
	printf("\n");
}

void DisplayArray(unsigned char *_array, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%d ", _array[i]);
	}
	printf("\n");
	printf("\n");
}

void Count(string file_name, const int process_num, const int process_count, const int round)
{
	long n;
	long number_length = 0;
	if (process_num == 0)
	{
		FILE *fp = NULL;
		fp = fopen(file_name.c_str(), "rb");
		if (fp == NULL)
		{
			printf("Cannot Open file %s\n", file_name.c_str());
			MPI_Finalize();
			exit(-1);
		}
		fseek(fp, 0, SEEK_END);
		long file_size = ftell(fp);
		n = file_size / sizeof(long long);
	}
	MPI_Bcast(&n, 1, MPI_LONG, 0, MPI_COMM_WORLD);

	/// 1. 均匀划分
	size_t number_length_in_per_process = n / process_count;
	size_t number_length_in_curr_process = process_num == process_count - 1 ? n - number_length_in_per_process * process_num : number_length_in_per_process;

	// 每个进程读取属于自己的一段数据
	MPI_File fh;
	size_t ierr;
	ierr = MPI_File_open(MPI_COMM_WORLD, file_name.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

	if (ierr)
	{
		printf("Cannot Open file %s\n", file_name.c_str());
		MPI_Finalize();
		exit(-1);
	}

	MPI_Status status;
	long long *numbers_in_curr_process = new long long[number_length_in_curr_process];
	MPI_File_seek(fh, sizeof(long long) * number_length_in_per_process * process_num, MPI_SEEK_SET);
	MPI_File_read(fh, numbers_in_curr_process, number_length_in_curr_process, MPI_LONG_LONG, &status);
	MPI_File_close(&fh);

	//DisplayArray(numbers_in_curr_process, number_length_in_curr_process);

	unsigned char *count_array = new unsigned char[ARRAY_LENGTH(process_count)]();
	

	for (size_t i = 0; i < number_length_in_curr_process; i++)
	{
		count_array[numbers_in_curr_process[i] - BOUNDARY(round, process_count)]++;
	}
	//DisplayArray(count_array, ARRAY_LENGTH(process_count));

	unsigned char *result = NULL;
	if (process_num == 0)
	{
		//printf("%lld %lld\n", ARRAY_LENGTH(process_count), BOUNDARY(round, process_count));
		result = new unsigned char[ARRAY_LENGTH(process_count)];
	}
	MPI_Reduce(count_array, result, ARRAY_LENGTH(process_count), MPI_UNSIGNED_CHAR, MPI_SUM, 0, MPI_COMM_WORLD);

	if (process_num == 0)
	{
		//DisplayArray(result, ARRAY_LENGTH(process_count));
		FILE *fp = NULL;
		fp = fopen("result", "ab+");
		if (fp == NULL)
		{
			printf("Cannot Open file %s\n", "result");
			MPI_Finalize();
			exit(-1);
		}

		fwrite(result, sizeof(unsigned char), ARRAY_LENGTH(process_count), fp);
		fclose(fp);
	}

	delete[] numbers_in_curr_process;
	delete[] count_array;
	delete[] result;
}

int main(int argc, char *argv[])
{
	if (argc != 3)
	{
		printf("Usage:%s dir round\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	string dir = argv[1];
	int round = atoi(argv[2]);

	int size, rank;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	//printf("process: %d of %d\n", rank, size);
	double start_time = MPI_Wtime();
	for (int i = 0; i < round; i++)
	{
		stringstream ss;
		ss << i;
		string file_index;
		ss >> file_index;
		string file_path = dir + "segment_" + file_index;

		Count(file_path, rank, size, i + 1);
		MPI_Barrier(MPI_COMM_WORLD);
	}
	start_time = MPI_Wtime() - start_time;
	double delta_time;
	MPI_Reduce(&start_time, &delta_time, 1, MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
	if (rank == 0)
		printf("%.15lfs\n\n", delta_time);

	MPI_Finalize();
	return 0;
}