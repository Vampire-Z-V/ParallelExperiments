#include "PSRS_OpenMP.h"
#include <omp.h>
#include <algorithm>

#ifdef _DEBUG
#include <iostream>
using namespace std;
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
	int *sample = new int[p*p];

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
			*(sample + sample_start_index++) = *(numbers + start_index + i * sample_offset);
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
		*(sample + i) = *(sample + (i + 1) * p);
	}
#pragma endregion

	// 暂时存放最终结果的数组
	int *result = new int[n];
	// 存放主元划分后的分界，p组，每组p个，最后一个为段结束的index
	int *flag_index = new int[p * p];
	// 记录每个段有多长，用于拷贝数据
	int *segment_length = new int[p - 1]();

#pragma region 6. 主元划分
#pragma omp parallel num_threads(p)
	{
		int thread_num = omp_get_thread_num();
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		int sample_index = 0;
		// 一共有p-1个分界线
		int flag_count = p - 1;
		int flag_start_index = thread_num * p;

		for (int i = start_index; i < end_index; i++)
		{
			// 当比主元大时，记录当前下标作为分界线
			if (*(numbers + i) > *(sample + sample_index))
			{
				// 计算当前段的长度，若是第一次计算，则用当前index - start_index，否则减去前一次记录的index
				int current_segment_length = sample_index == 0 ? i - start_index : i - *(flag_index + flag_start_index + sample_index - 1);
#pragma omp atomic
				*(segment_length + sample_index) += current_segment_length;

				*(flag_index + flag_start_index + sample_index) = i;

				sample_index++;
				if (sample_index == flag_count)
					break;
			}
		}

		// 记录段结束的索引
		*(flag_index + flag_start_index + sample_index) = end_index;
#pragma endregion

#pragma omp barrier 

#pragma omp master
		for (int i = 1; i < p - 1; i++)
		{
			segment_length[i] += segment_length[i - 1];
		}
#pragma omp barrier 

		int result_start_index = thread_num == 0 ? 0 : *(segment_length + thread_num - 1);
		int result_start_index_backup = result_start_index;

#pragma region 7. 全局交换
		for (int i = 0; i < p; i++)
		{
			int copy_start_index = thread_num == 0 && i == 0 ? 0 : *(flag_index + i * p + thread_num - 1);
			int copy_end_index = *(flag_index + i * p + thread_num);
			int length = copy_end_index - copy_start_index;
			memcpy(result + result_start_index, numbers + copy_start_index, length * sizeof(int));
			result_start_index += length;
		}
#pragma endregion

#pragma region 8. 归并排序
		// 此时result_start_index已经被移到了该段的末尾
		sort(result + result_start_index_backup, result + result_start_index);
#pragma endregion
	}

	delete[] sample;
	delete[] flag_index;
	delete[] segment_length;

	return result;
}
