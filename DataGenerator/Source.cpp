#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define BLOCK 1
double data[BLOCK];

int main(int argc, char* argv[])
{
	srand(time(0));
	if (argc != 5)
	{
		printf("Usage:%s start_value loop_times data_file use_rand(1/0)\n", argv[0]);
	}

	double start = atoi(argv[1]);
	int loop = atoi(argv[2]);

	printf("starte_value=%d loop=%d\n", start, loop);
	FILE *fp = NULL;
	fopen_s(&fp, argv[3], "wb");

	int useRand = atoi(argv[4]);

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", argv[3]);
		return -1;
	}

	double s = start;

	for (int i = 0; i < loop; i++)
	{
		for (auto &d : data)
		{
			if (useRand == 0)
			{
				d = s--;
			}
			else
			{
				d = rand();
			}
		}

		fwrite(data, sizeof(double)*BLOCK, 1, fp);
	}
	fclose(fp);
}