#include <stdlib.h>
#include <stdio.h>

#define BLOCK 1
int data[BLOCK];

int main(int argc, char* argv[])
{
	if (argc != 4)
	{
		printf("Usage:%s start_value loop_times data_file\n", argv[0]);
	}

	int start = atoi(argv[1]);
	int loop = atoi(argv[2]);

	printf("starte_value=%d loop=%d\n", start, loop);
	FILE *fp = NULL;
	fopen_s(&fp, argv[3], "wb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", argv[3]);
		return -1;
	}

	int s = (int)start;

	for (int i = 0; i < loop; i++)
	{
		for (auto &d : data)
			d = s--;

		fwrite(data, sizeof(int)*BLOCK, 1, fp);
	}
	fclose(fp);
}