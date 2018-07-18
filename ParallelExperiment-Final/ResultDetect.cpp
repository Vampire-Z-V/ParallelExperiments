#include <stdlib.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		printf("Usage:%s number", argv[0]);
		exit(EXIT_FAILURE);
	}

	FILE *fp = NULL;
	fp = fopen("result", "rb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", "result");
		exit(-1);
	}

	long offset = atol(argv[1]);

	fseek(fp, offset, SEEK_SET);

	unsigned char count;

	fread(&count, sizeof(unsigned char), 1, fp);

	fclose(fp);

	printf("%lld: %d\n", offset, count);

	return 0;
}