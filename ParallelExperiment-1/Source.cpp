#include <stdio.h>
#include <omp.h>
#include <algorithm>
#include "PSRS_OpenMP.h"

using namespace std;

#define TIME_START_RECORD \
	double StartTime = omp_get_wtime();
#define TIME_STOP_RECORD \
	double EndTime = omp_get_wtime(); \
	printf("RunTime: %.10fs\n", EndTime - StartTime);
#define BLOCK 200

int main()
{
	/*int test[] = {
		33,78,92,1,39,19,71,39,90,3,21,4,82,87,89,76,31,10,24,53,50,76,33,78,56,13,15,68,94,93,8,36,63,82,40,56,81,25,29,77,65,54,27,82,24,99,41,5,92,43
	};*/
	int *test = new int[BLOCK]();
	for (int i = 1; i < BLOCK; i++)
	{
		test[i] = test[i - 1] - 1;
	}
	/*for (int i = 0; i < BLOCK; i++)
	{
		printf("%d ", test[i]);
	}
	printf("\n");
	printf("\n");*/

	TIME_START_RECORD
	int * result = PSRS(test, BLOCK, 10);
	TIME_STOP_RECORD

//#ifdef _DEBUG
	/*for (int i = 0; i < BLOCK; i++)
	{
		printf("%d ", test[i]);
	}
	printf("\n");*/
	system("pause");
//#endif 

	delete[] test;
	test = nullptr;
	//delete[] result;

	return 0;
}