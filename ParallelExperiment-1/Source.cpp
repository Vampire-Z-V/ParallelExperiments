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

int main()
{
	/*int test[] = {
		33,78,92,1,39,19,71,39,90,3,21,4,82,87,89,76,31,10,24,53,50,76,33,78,56,13,15,68,94,93,8,36,63,82,40,56,81,25,29,77,65,54,27,82,24,99,41,5,92,43
	};*/
	int test[] = {
		15,46,48,93,39,6,72,91,14,36,69,40,89,61,97,12,21,54,53,97,84,58,32,27,33,72,20
	};
	int length = sizeof(test) / sizeof(int);

	TIME_START_RECORD
	int * result = PSRS(test, length, 4);
	TIME_STOP_RECORD

#ifdef _DEBUG
	/*for (auto t : test)
	{
		printf("%d ", t);
	}
	printf("\n");*/

	for (int i = 0; i < length; i++)
	{
		printf("%d ", result[i]);
	}
	printf("\n");
	system("pause");
#endif 

	if(result != test)
		delete[] result;

	return 0;
}