#include "PSRS_MPI.h"
#include <mpi.h>
#include <algorithm>
#include <vector>
using namespace std;

#ifdef _DEBUG
#include <stdio.h>
#include <stdlib.h>
#endif // _DEBUG

int* PSRS(const char* dataFilePath, int n, int process_num, int process_count)
{
	MPI_Request request;

#pragma region 1. 均匀划分
	int number_length_in_per_process = n / process_count;
	int number_length_in_curr_process = process_num == process_count - 1 ? n - number_length_in_per_process * process_num : number_length_in_per_process;

	// 每个进程读取属于自己的一段数据
	MPI_File fh;
	int ierr;
	ierr = MPI_File_open(MPI_COMM_WORLD, dataFilePath, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

	if (ierr)
	{
		printf("Cannot Open file %s\n", dataFilePath);
		MPI_Finalize();
		exit(-1);
	}

	MPI_Status status;
	int *numbers_in_curr_process = new int[number_length_in_curr_process];
	MPI_File_seek(fh, sizeof(int) * number_length_in_per_process * process_num, MPI_SEEK_SET);
	MPI_File_read(fh, numbers_in_curr_process, number_length_in_curr_process, MPI_INT, &status);
	MPI_File_close(&fh);
#pragma endregion

#pragma region 2. 局部排序
	sort(numbers_in_curr_process, numbers_in_curr_process + number_length_in_curr_process);
#pragma endregion

#pragma region 3. 正则采样
	int *sample = nullptr;
	int *temp = new int[process_count];

	// 现在temp用于存放各个进程自己的采样，长度process_count
	int sample_offset = number_length_in_curr_process / process_count;
	for (int i = 0; i < process_count; i++)
	{
		temp[i] = numbers_in_curr_process[i * sample_offset];
	}

	if (process_num == 0)
	{
		// 主进程负责从各个进程中接收采样，存在sample中
		sample = new int[process_count * process_count];
		memcpy(sample, temp, process_count * sizeof(int));

		for (int i = 1; i < process_count; i++)
		{
			MPI_Recv(sample + i * process_count, process_count, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}

#pragma region 4. 采样排序
		sort(sample, sample + process_count * process_count);
#pragma endregion

#pragma region 5. 选择主元
		// 现在temp用于存放主元数据，长度process_count - 1
		for (int i = 0; i < process_count - 1; i++)
		{
			temp[i] = sample[(i + 1) * process_count];
		}

		// 主进程向各个进程发送主元
		for (int i = 1; i < process_count; i++)
		{
			MPI_Isend(temp, process_count - 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
#pragma endregion
	}
	else
	{
		// 非主进程先发送自己的采样，然后从主进程处接收主元信息

		// 现在temp存放各个进程自己的采样，长度process_count
		MPI_Send(temp, process_count, MPI_INT, 0, 0, MPI_COMM_WORLD);
		// 现在temp存放主进程发送的主元，长度process_count - 1
		MPI_Recv(temp, process_count - 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}

	//#ifdef _DEBUG
	//	for (int i = 0; i < process_count - 1; i++)
	//	{
	//		printf("%d ", temp[i]);
	//	}
	//	printf("\n");
	//#endif // _DEBUG
#pragma endregion

#pragma region 6. 主元划分
	int flag_index = 0;
	// 现在temp记录numbers_in_curr_process中第一个比主元大的数的下标，长度process_count
	for (int i = 0; i < number_length_in_curr_process; i++)
	{
		if (numbers_in_curr_process[i] > temp[flag_index])
		{
			do
			{
				temp[flag_index++] = i;
			} while (numbers_in_curr_process[i] > temp[flag_index] && flag_index < process_count - 1);

			if (flag_index == process_count - 1)
				break;
		}
	}
	// 剩下的记为number_length_in_curr_process
	while (flag_index < process_count)
	{
		temp[flag_index++] = number_length_in_curr_process;
	}

	// 现在temp记录每段的长度，长度process_count
	for (int i = process_count - 1; i > 0; i--)
	{
		temp[i] -= temp[i - 1];
	}

	// 将对应段的长度发送给对应的进程，给自己的不发送
	int *receive_data_length = new int[process_count]();
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Isend(temp + i, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
	}

	receive_data_length[process_num] = temp[process_num];
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Recv(receive_data_length + i, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}
	//#ifdef _DEBUG
	//	for (int i = 0; i < process_count; i++)
	//	{
	//		printf("%d ", temp[i]);
	//	}
	//	printf("\n");
	//#endif // _DEBUG
#pragma endregion

#pragma region 7. 全局交换
	// 将数据发到对应进程
	int start_index = 0;
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num && temp[i] != 0)
		{
			MPI_Isend(numbers_in_curr_process + start_index, temp[i], MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
		start_index += temp[i];
	}

	int result_length = 0;
	for (int i = 0; i < process_count; i++)
	{
		result_length += receive_data_length[i];
	}

	int *result_temp = new int[result_length];
	vector<int> segment_start_indices;
	start_index = 0;
	for (int i = 0; i < process_count; i++)
	{
		if (receive_data_length[i] == 0)
			continue;

		if (i != process_num)
		{
			MPI_Recv(result_temp + start_index, receive_data_length[i], MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
		else
		{
			memcpy(result_temp + start_index, numbers_in_curr_process, receive_data_length[i] * sizeof(int));
		}
		segment_start_indices.push_back(start_index);
		start_index += receive_data_length[i];
	}
	delete[] numbers_in_curr_process;
	segment_start_indices.push_back(start_index);
	//for (auto i : segment_start_indices)
	//{
	//	printf("%d ", i);
	//}
	//printf("\n");
#pragma endregion

#pragma region 8. 归并排序
	//sort(result_temp, result_temp + result_length);
	vector<int> points(segment_start_indices);
	int min = INT_MAX;
	int *min_index = nullptr;
	int count = 0;
	numbers_in_curr_process = new int[result_length];

	while (count < result_length)
	{
		for (int i = 0; i < points.size() - 1; i++)
		{
			if (points[i] < segment_start_indices[i + 1] && result_temp[points[i]] < min)
			{
				min = result_temp[points[i]];
				min_index = &points[i];
			}
		}

		numbers_in_curr_process[count++] = min;
		(*min_index)++;
		min = INT_MAX;
	}
#pragma endregion

#ifdef _DEBUG
	for (int i = 0; i < result_length; i++)
	{
		printf("%d ", numbers_in_curr_process[i]);
	}
	printf("\n");

	for (int i = 0; i < result_length - 1; i++)
	{
		if (numbers_in_curr_process[i] > numbers_in_curr_process[i + 1])
		{
			printf("sort error");
			goto end;
		}
	}
	printf("sort success");
	printf("\n");
	printf("\n");
#endif // _DEBUG

end:
	delete[] sample;
	delete[] temp;
	delete[] receive_data_length;
	delete[] result_temp;
	return numbers_in_curr_process;
}
