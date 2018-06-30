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
	// ÿ�����ݵĳ��ȣ����һ�ο��ܻ᲻һ��
	int offset = n / p;
	// Ϊ�����������ռ�
	int *sample = new int[p*p];

#pragma omp parallel num_threads(p)
	{
#pragma region 1. ���Ȼ���
		int thread_num = omp_get_thread_num();
		// �����ÿ���߳���Ҫ����Ķε���ʼ���յ�λ�ã��Լ�����ĳ���
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;
		int length = end_index - start_index;
#pragma endregion

#pragma region 2. �ֲ�����
		sort(numbers + start_index, numbers + end_index);
#pragma endregion

#pragma region 3. �������
		// �����ÿ���̴߳�Ų������ݵ���ʼλ�úͲ��������ÿ���̶߳��ܻ�ȡp������
		int sample_start_index = thread_num * p;
		int sample_offset = length / p;
		for (int i = 0; i < p; i++)
		{
			*(sample + sample_start_index++) = *(numbers + start_index + i * sample_offset);
		}
#pragma endregion
	}

#pragma omp barrier 

#pragma region 4. ��������
	sort(sample, sample + p * p);
#pragma endregion

#pragma region 5. ѡ����Ԫ
	// ѡ����Ԫ������ڸոմ�Ų������ݵ�ǰ�棬��ʡ�ռ�
	for (int i = 0; i < p - 1; i++)
	{
		*(sample + i) = *(sample + (i + 1) * p);
	}
#pragma endregion

	// ��ʱ������ս��������
	int *result = new int[n];
	// �����Ԫ���ֺ�ķֽ磬p�飬ÿ��p�������һ��Ϊ�ν�����index
	int *flag_index = new int[p * p];
	// ��¼ÿ�����ж೤�����ڿ�������
	int *segment_length = new int[p - 1]();

#pragma region 6. ��Ԫ����
#pragma omp parallel num_threads(p)
	{
		int thread_num = omp_get_thread_num();
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		int sample_index = 0;
		// һ����p-1���ֽ���
		int flag_count = p - 1;
		int flag_start_index = thread_num * p;

		for (int i = start_index; i < end_index; i++)
		{
			// ������Ԫ��ʱ����¼��ǰ�±���Ϊ�ֽ���
			if (*(numbers + i) > *(sample + sample_index))
			{
				// ���㵱ǰ�εĳ��ȣ����ǵ�һ�μ��㣬���õ�ǰindex - start_index�������ȥǰһ�μ�¼��index
				int current_segment_length = sample_index == 0 ? i - start_index : i - *(flag_index + flag_start_index + sample_index - 1);
#pragma omp atomic
				*(segment_length + sample_index) += current_segment_length;

				*(flag_index + flag_start_index + sample_index) = i;

				sample_index++;
				if (sample_index == flag_count)
					break;
			}
		}

		// ��¼�ν���������
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

#pragma region 7. ȫ�ֽ���
		for (int i = 0; i < p; i++)
		{
			int copy_start_index = thread_num == 0 && i == 0 ? 0 : *(flag_index + i * p + thread_num - 1);
			int copy_end_index = *(flag_index + i * p + thread_num);
			int length = copy_end_index - copy_start_index;
			memcpy(result + result_start_index, numbers + copy_start_index, length * sizeof(int));
			result_start_index += length;
		}
#pragma endregion

#pragma region 8. �鲢����
		// ��ʱresult_start_index�Ѿ����Ƶ��˸öε�ĩβ
		sort(result + result_start_index_backup, result + result_start_index);
#pragma endregion
	}

	delete[] sample;
	delete[] flag_index;
	delete[] segment_length;

	return result;
}
