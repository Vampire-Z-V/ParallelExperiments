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
#define BLOCK 200

int main()
{
	//srand(time(0));

	int *test = new int[BLOCK]();
	test[0] = BLOCK;
	for (int i = 1; i < BLOCK; i++)
	{
		test[i] = rand() % BLOCK;
		//test[i] = test[i - 1] - 1;
	}

	TIME_START_RECORD;
	PSRS(test, BLOCK, 10);
	TIME_STOP_RECORD;

	for (int i = 1; i < BLOCK; i++)
	{
		if (test[i] < test[i - 1])
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
	system("pause");
	//#endif 

	delete[] test;
	test = nullptr;
	//delete[] result;

	return 0;
}