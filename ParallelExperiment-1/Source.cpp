#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
#include <algorithm>
#include "PSRS_OpenMP.h"

using namespace std;

#define TIME_START_RECORD \
	double StartTime = omp_get_wtime();
#define TIME_STOP_RECORD \
	double EndTime = omp_get_wtime(); \
	printf("RunTime: %.10fs\n", EndTime - StartTime);
#define BLOCK 1024*1024

int main(int argc, char *argv[])
{

	if (argc != 4)
	{
		printf("Usage:%s loop_times data_file thread_num\n", argv[0]);
		exit(EXIT_FAILURE);
	}
	//srand(time(0));

	//int *test = new int[BLOCK]();
	//test[0] = BLOCK;
	//for (int i = 1; i < BLOCK; i++)
	//{
	//	test[i] = rand() % BLOCK;
	//	//test[i] = test[i - 1] - 1;
	//}

	int loop = atoi(argv[1]);
	int number_total_length = BLOCK * loop;

	FILE *fp = NULL;
	fp = fopen(argv[2], "rb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", argv[2]);
		exit(-1);
	}

	int *numbers = new int[number_total_length];
	fread(numbers, sizeof(int), number_total_length, fp);
	fclose(fp);

	TIME_START_RECORD;
	PSRS(numbers, number_total_length, atoi(argv[3]));
	TIME_STOP_RECORD;

	for (int i = 1; i < number_total_length; i++)
	{
		if (numbers[i] < numbers[i - 1])
		{
			printf("sort fail\n");
			goto end;
		}
	}
	printf("sort success\n");
	//#ifdef _DEBUG
		/*for (int i = 0; i < BLOCK; i++)
		{
			printf("%d ", test[i]);
		}
		printf("\n");*/
end:
	//#endif 

	delete[] numbers;

	return 0;
}