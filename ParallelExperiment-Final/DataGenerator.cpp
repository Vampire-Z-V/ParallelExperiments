//#include <stdlib.h>
//#include <stdio.h>
//#include <time.h>
//#include <string>
//#include <sstream>
//#include <iostream>
//using namespace std;
//
//#define BLOCK 1
//int64_t data_temp[BLOCK];
//
//int main(int argc, char* argv[])
//{
//	srand(time(0));
//	if (argc != 4)
//	{
//		printf("Usage:%s start_value loop_times use_rand(1/0)\n", argv[0]);
//	}
//
//	int start = atoi(argv[1]);
//	int loop = atoi(argv[2]);
//
//	printf("starte_value=%d loop=%d\n", start, loop);
//	FILE *fp = NULL;
//
//	int useRand = atoi(argv[3]);
//
//	string prefix = ".txt";
//	for (int i = 1; i <= 64; i++)
//	{
//		string number;
//		stringstream ss;
//		ss << i;
//		ss >> number;
//		string file_name = number + prefix;
//
//		fopen_s(&fp, file_name.c_str(), "wb");
//		if (fp == NULL)
//		{
//			printf("Cannot Open file %s\n", file_name.c_str());
//			return -1;
//		}
//
//		for (int i = 0; i < loop; i++)
//		{
//			for (auto &d : data_temp)
//			{
//				if (useRand == 0)
//				{
//					d = start--;
//				}
//				else
//				{
//					d = rand();
//				}
//			}
//
//			fwrite(data_temp, sizeof(int64_t)*BLOCK, 1, fp);
//		}
//		fclose(fp);
//	}
//}