#include "PSRS_OpenMP.h"
#include <omp.h>
#include <algorithm>
using namespace std;

#ifdef _DEBUG
#include <iostream>
#endif

int * PSRS(int *numbers, int n, int p)
{
	if (p <= 0)
	{
		return nullptr;
	}
	else if (p == 1)
	{
		sort(numbers, numbers + n);
		return numbers;
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

	for (int i = 0; i < p - 1; i++)
	{
		printf("%d ", sample[i]);
	}
	printf("\n");

	// 暂时存放最终结果的数组
	int *result = new int[n];
	// 存放主元划分后的分界，p组，每组p个，最后一个为段结束的index
	int *temp = new int[p * p];
	// 记录每个段有多长，用于拷贝数据
	int *segment_length = new int[p]();

#pragma region 6. 主元划分
#pragma omp parallel num_threads(p)
	{
		int thread_num = omp_get_thread_num();
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		int sample_index = 0;
		const int flag_start_index = thread_num * p;

		// 将每一段数据按照主元划分成小段，记录分界index
		for (int i = start_index; i < end_index; i++)
		{
			if (numbers[i] > sample[sample_index])
			{
				do
				{
					temp[flag_start_index + sample_index++] = i;
				} while (numbers[i] > sample[sample_index] && sample_index < p - 1);

				if (sample_index == p - 1)
					break;
			}
		}
		while (sample_index < p)
		{
			temp[flag_start_index + sample_index++] = end_index;
		}

#pragma omp barrier 
#pragma omp master
		{
			for (int i = 0; i < p; i++)
			{
				for (int j = 0; j < p; j++)
				{
					if (i == 0 && j == 0)
						segment_length[0] += temp[0];
					else
					{
						segment_length[i] += temp[i*p + j] - temp[i*p + j - 1];
					}
				}
			}

			for (int i = 1; i < p; i++)
			{
				segment_length[i] += segment_length[i - 1];
			}

			for (int i = 0; i < p; i++)
			{
				printf("%d ", segment_length[i]);
			}
			printf("\n");
		}

		//for (int i = 0; i < n; i++)
		//{
		//	printf("%d ", numbers[i]);
		//}
		//printf("\n");
		//for (int i = 0; i < p * p; i++)
		//{
		//	printf("%d ", temp[i]);
		//}
		//printf("\n");
		///*for (int i = p * p - 1; i > 0; i--)
		//{
		//	temp[i] -= temp[i - 1];
		//}*/

		
#pragma endregion

		int result_start_index = thread_num == 0 ? 0 : segment_length[thread_num - 1];
		int result_start_index_backup = result_start_index;

#pragma region 7. 全局交换
		for (int i = 0; i < p; i++)
		{
			int copy_start_index = thread_num == 0 && i == 0 ? 0 : temp[i * p + thread_num - 1];
			int copy_end_index = temp[i * p + thread_num];
			int length = copy_end_index - copy_start_index;
			if (length != 0)
				memcpy(result + result_start_index, numbers + copy_start_index, length * sizeof(int));
			result_start_index += length;
		}
#pragma endregion

#pragma region 8. 归并排序
		// 此时result_start_index已经被移到了该段的末尾
		sort(result + result_start_index_backup, result + result_start_index);
#pragma endregion
	}

	for (int i = 0; i < n; i++)
	{
		printf("%d ", result[i]);
	}
	printf("\n");

	delete[] sample;
	delete[] temp;
	delete[] segment_length;

	return result;
}
