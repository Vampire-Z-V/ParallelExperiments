#include <stdlib.h>
#include <stdio.h>

#define BLOCK 1024*1024
int data[BLOCK];

int main(int argc, char* argv[])
{
	if (argc != 5)
	{
		printf("Usage:%s start_value loop_times data_file use_rand(1/0)\n", argv[0]);
	}

	int start = atoi(argv[1]);
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

	int s = (int)start;

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
				d = rand() % (loop * BLOCK);
			}
		}

		fwrite(data, sizeof(int)*BLOCK, 1, fp);
	}
	fclose(fp);
}