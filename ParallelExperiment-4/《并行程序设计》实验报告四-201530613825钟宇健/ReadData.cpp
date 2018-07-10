#include <stdlib.h>
#include <stdio.h>

void DisplayArray(double *_array, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%.0lf ", _array[i]);
	}
	printf("\n");
	printf("\n");
}

void ReadTempFile(const char* file_name, const size_t length)
{
	FILE *fp = NULL;
	fp = fopen(file_name, "rb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", file_name);
		exit(-1);
	}

	double *numbers = new double[length * 2];
	fread(numbers, sizeof(double), length, fp);
	fseek(fp, -length * sizeof(double), SEEK_END);
	fread(numbers + length, sizeof(double), length, fp);
	fclose(fp);

	printf("First %lld:\n", length);
	DisplayArray(numbers, length);
	printf("Post %lld:\n", length);
	DisplayArray(numbers + length, length);
	delete[] numbers;
}

void ReadTempFile(const char* file_name)
{
	FILE *fp = NULL;
	fp = fopen(file_name, "rb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", file_name);
		exit(-1);
	}

	fseek(fp, 0, SEEK_END);
	long file_size = ftell(fp);
	long number_length = file_size / sizeof(double);
	printf("count: %ld\n", number_length);
	fseek(fp, 0, SEEK_SET);

	double *numbers = new double[number_length];
	fread(numbers, sizeof(double), number_length, fp);
	fclose(fp);

	DisplayArray(numbers, number_length);
	delete[] numbers;
}

int main(int argc, char *argv[])
{
	if (argc < 2 || argc > 3)
	{
		printf("Usage:%s data_file\nor\nUsage:%s data_file view_data_count", argv[0]);
		exit(EXIT_FAILURE);
	}
	if (argc == 2)
	{
		ReadTempFile(argv[1]);
	}
	else
	{
		ReadTempFile(argv[1], atoll(argv[2]));
	}
	return 0;
}