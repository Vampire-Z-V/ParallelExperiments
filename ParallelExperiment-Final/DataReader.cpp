//#include <stdlib.h>
//#include <stdio.h>
//
//void DisplayArray(long long *_array, size_t length)
//{
//	for (size_t i = 0; i < length; i++)
//	{
//		printf("%lld ", _array[i]);
//	}
//	printf("\n");
//	printf("\n");
//}
//
//void ReadTempFile(const char* file_name, const size_t length)
//{
//	FILE *fp = NULL;
//	fp = fopen(file_name, "rb");
//
//	if (fp == NULL)
//	{
//		printf("Cannot Open file %s\n", file_name);
//		exit(-1);
//	}
//
//	long long *numbers = new long long[length * 2];
//	fread(numbers, sizeof(long long), length, fp);
//	fseek(fp, -length * sizeof(long long), SEEK_END);
//	fread(numbers + length, sizeof(long long), length, fp);
//	fclose(fp);
//
//	printf("First %lld:\n", length);
//	DisplayArray(numbers, length);
//	printf("Post %lld:\n", length);
//	DisplayArray(numbers + length, length);
//	delete[] numbers;
//}
//
//void ReadTempFile(const char* file_name)
//{
//	FILE *fp = NULL;
//	fp = fopen(file_name, "rb");
//
//	if (fp == NULL)
//	{
//		printf("Cannot Open file %s\n", file_name);
//		exit(-1);
//	}
//
//	fseek(fp, 0, SEEK_END);
//	long file_size = ftell(fp);
//	long number_length = file_size / sizeof(long long);
//	printf("count: %ld\n", number_length);
//	fseek(fp, 0, SEEK_SET);
//
//	long long *numbers = new long long[number_length];
//	fread(numbers, sizeof(long long), number_length, fp);
//	fclose(fp);
//
//	DisplayArray(numbers, number_length);
//	delete[] numbers;
//}
//
//int main(int argc, char *argv[])
//{
//	if (argc < 2 || argc > 3)
//	{
//		printf("Usage:%s data_file\nor\nUsage:%s data_file view_data_count", argv[0]);
//		exit(EXIT_FAILURE);
//	}
//	if (argc == 2)
//	{
//		ReadTempFile(argv[1]);
//	}
//	else
//	{
//		ReadTempFile(argv[1], atoll(argv[2]));
//	}
//	return 0;
//}