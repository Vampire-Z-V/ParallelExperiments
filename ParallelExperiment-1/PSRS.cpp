#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
#include <algorithm>
#include <vector>

using namespace std;

#define TIME_START_RECORD \
	double StartTime = omp_get_wtime();
#define TIME_STOP_RECORD \
	double EndTime = omp_get_wtime(); \
	printf("RunTime: %.10fs\n", EndTime - StartTime);
#define BLOCK 1024*1024

void PSRS(int *numbers, int n, int p)
{
	if (p <= 0)
	{
		return;
	}
	else if (p == 1)
	{
		sort(numbers, numbers + n);
		return;
	}
	// 每段数据的长度，最后一段可能会不一样
	int offset = n / p;
	// 为正则采样分配空间
	int *sample = new int[p * p];

#pragma omp parallel num_threads(p)
	{
#pragma region 1. 均匀划分
		int thread_num = omp_get_thread_num();
		// 计算出每个线程需要处理的段的起始和终点位置，以及处理的长度
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;
		int length = end_index - start_index;
#pragma endregion

#pragma region 2. 局部排序
		sort(numbers + start_index, numbers + end_index);
#pragma endregion

#pragma region 3. 正则采样
		// 计算出每个线程存放采样数据的起始位置和采样间隔，每个线程都能获取p个数据
		int sample_start_index = thread_num * p;
		int sample_offset = length / p;
		for (int i = 0; i < p; i++)
		{
			sample[sample_start_index++] = numbers[start_index + i * sample_offset];
		}
#pragma endregion
	}
#pragma omp barrier 

#pragma region 4. 采样排序
	sort(sample, sample + p * p);
#pragma endregion

#pragma region 5. 选择主元
	// 选择主元，存放在刚刚存放采样数据的前面，节省空间
	for (int i = 0; i < p - 1; i++)
	{
		sample[i] = sample[(i + 1) * p];
	}
#pragma endregion
	// 暂时存放最终结果的数组
	int *result_temp = new int[n];
	// 存放主元划分后的分界，p组，每组p个，最后一个为段结束的index
	int *slice_indices = new int[p * p];
	// 记录每个段有多长，用于拷贝数据
	int *segment_length = new int[p]();

#pragma omp parallel num_threads(p)
	{
#pragma region 6. 主元划分
		int thread_num = omp_get_thread_num();
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		int sample_index = 0;
		const int slice_start_index = thread_num * p;

		// 将每一段数据按照主元划分成小段，记录分界index
		for (int i = start_index; i < end_index; i++)
		{
			if (numbers[i] > sample[sample_index])
			{
				do
				{
					slice_indices[slice_start_index + sample_index++] = i;
				} while (numbers[i] > sample[sample_index] && sample_index < p - 1);

				if (sample_index == p - 1)
					break;
			}
		}
		while (sample_index < p)
		{
			slice_indices[slice_start_index + sample_index++] = end_index;
		}

#pragma omp barrier 
#pragma omp master
		{
			for (int i = 0; i < p; i++)
			{
				for (int j = 0; j < p; j++)
				{
					if (i == 0 && j == 0)
						segment_length[0] += slice_indices[0];
					else
					{
						segment_length[i] += slice_indices[j*p + i] - slice_indices[j*p + i - 1];
					}
				}
			}

			for (int i = 1; i < p; i++)
			{
				segment_length[i] += segment_length[i - 1];
			}
		}

#pragma omp barrier 
#pragma endregion

#pragma region 7. 全局交换
		int result_start_index = thread_num == 0 ? 0 : segment_length[thread_num - 1];
		int result_start_index_backup = result_start_index;
		vector<int> segment_start_indices;

		for (int i = 0; i < p; i++)
		{
			int copy_start_index = thread_num == 0 && i == 0 ? 0 : slice_indices[i * p + thread_num - 1];
			int copy_end_index = slice_indices[i * p + thread_num];
			int length = copy_end_index - copy_start_index;
			if (length != 0)
			{
				segment_start_indices.push_back(result_start_index);
				memcpy(result_temp + result_start_index, numbers + copy_start_index, length * sizeof(int));
				result_start_index += length;
			}
		}
		int result_end_index = result_start_index;
		result_start_index = result_start_index_backup;
#pragma omp barrier 
#pragma endregion

#pragma region 8. 归并排序
		vector<int> points(segment_start_indices);
		int min = INT_MAX;
		int *min_index = nullptr;
		int count = 0;
		int length = result_end_index - result_start_index;

		while (count < length)
		{
			int i = 0;
			for (; i < points.size() - 1; i++)
			{
				if (points[i] < segment_start_indices[i + 1] && result_temp[points[i]] < min)
				{
					min = result_temp[points[i]];
					min_index = &points[i];
				}
			}
			if (points[i] < result_end_index && result_temp[points[i]] < min)
			{
				min = result_temp[points[i]];
				min_index = &points[i];
			}

			count++;
			numbers[result_start_index++] = min;
			(*min_index)++;
			min = INT_MAX;
		}
#pragma endregion
	}

	delete[] sample;
	delete[] slice_indices;
	delete[] segment_length;
	delete[] result_temp;
}


int main(int argc, char *argv[])
{

	if (argc != 4)
	{
		printf("Usage:%s loop_times data_file thread_num\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	int loop = atoi(argv[1]);
	int number_total_length = BLOCK * loop;

	FILE *fp = NULL;
	fp = fopen(argv[2], "rb");

	if (fp == NULL)
	{
		printf("Cannot Open file %s\n", argv[2]);
		exit(-1);
	}

	int *numbers = new int[number_total_length];
	fread(numbers, sizeof(int), number_total_length, fp);
	fclose(fp);

	TIME_START_RECORD;
	PSRS(numbers, number_total_length, atoi(argv[3]));
	TIME_STOP_RECORD;

	for (int i = 1; i < number_total_length; i++)
	{
		if (numbers[i] < numbers[i - 1])
		{
			printf("sort fail\n");
			goto end;
		}
	}
	printf("sort success\n");
end:
	//#endif 

	delete[] numbers;

	return 0;
}