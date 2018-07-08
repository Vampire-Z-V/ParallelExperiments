#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <vector>
#include <mpi.h>
#include <algorithm>
using namespace std;


#define BLOCK 1024*1024
int* PSRS(const char* dataFilePath, int n, int process_num, int process_count)
{
	MPI_Request request;

#pragma region 1. ���Ȼ���
	int number_length_in_per_process = n / process_count;
	int number_length_in_curr_process = process_num == process_count - 1 ? n - number_length_in_per_process * process_num : number_length_in_per_process;

	// ÿ�����̶�ȡ�����Լ���һ������
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
	double start_time = MPI_Wtime();

#pragma region 2. �ֲ�����
	sort(numbers_in_curr_process, numbers_in_curr_process + number_length_in_curr_process);
	if (process_count == 1)
	{
#ifdef _DEBUG
		for (int i = 0; i < number_length_in_curr_process; i++)
		{
			printf("%d ", numbers_in_curr_process[i]);
		}
		printf("\n");

		for (int i = 0; i < number_length_in_curr_process - 1; i++)
		{
			if (numbers_in_curr_process[i] > numbers_in_curr_process[i + 1])
			{
				printf("sort error");
				goto end_first;
			}
		}
		printf("sort success");
		printf("\n");
		printf("\n");
#endif // _DEBUG

	end_first:
		return numbers_in_curr_process;
	}
#pragma endregion

#pragma region 3. �������
	int *sample = nullptr;
	int *temp = new int[process_count];

	// ����temp���ڴ�Ÿ��������Լ��Ĳ���������process_count
	int sample_offset = number_length_in_curr_process / process_count;
	for (int i = 0; i < process_count; i++)
	{
		temp[i] = numbers_in_curr_process[i * sample_offset];
	}

	if (process_num == 0)
	{
		// �����̸���Ӹ��������н��ղ���������sample��
		sample = new int[process_count * process_count];
		memcpy(sample, temp, process_count * sizeof(int));

		for (int i = 1; i < process_count; i++)
		{
			MPI_Recv(sample + i * process_count, process_count, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}

#pragma region 4. ��������
		sort(sample, sample + process_count * process_count);
#pragma endregion

#pragma region 5. ѡ����Ԫ
		// ����temp���ڴ����Ԫ���ݣ�����process_count - 1
		for (int i = 0; i < process_count - 1; i++)
		{
			temp[i] = sample[(i + 1) * process_count];
		}

		// ��������������̷�����Ԫ
		for (int i = 1; i < process_count; i++)
		{
			MPI_Isend(temp, process_count - 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
#pragma endregion
	}
	else
	{
		// ���������ȷ����Լ��Ĳ�����Ȼ��������̴�������Ԫ��Ϣ

		// ����temp��Ÿ��������Լ��Ĳ���������process_count
		MPI_Send(temp, process_count, MPI_INT, 0, 0, MPI_COMM_WORLD);
		// ����temp��������̷��͵���Ԫ������process_count - 1
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

#pragma region 6. ��Ԫ����
	int flag_index = 0;
	// ����temp��¼numbers_in_curr_process�е�һ������Ԫ��������±꣬����process_count
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
	// ʣ�µļ�Ϊnumber_length_in_curr_process
	while (flag_index < process_count)
	{
		temp[flag_index++] = number_length_in_curr_process;
	}

	// ����temp��¼ÿ�εĳ��ȣ�����process_count
	for (int i = process_count - 1; i > 0; i--)
	{
		temp[i] -= temp[i - 1];
	}

	// ����Ӧ�εĳ��ȷ��͸���Ӧ�Ľ��̣����Լ��Ĳ�����
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

#pragma region 7. ȫ�ֽ���
	// �����ݷ�����Ӧ����
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

#pragma region 8. �鲢����
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
	printf("%.10lfs\n", MPI_Wtime() - start_time);

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

end:
	delete[] sample;
	delete[] temp;
	delete[] receive_data_length;
	delete[] result_temp;
	return numbers_in_curr_process;
}

int main(int argc, char *argv[])
{
	if (argc != 3)
	{
		printf("Usage:%s loop_times data_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	// ��ʼ��MPI
	int size, rank;
	MPI_Init(nullptr, nullptr);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	printf("process: %d of %d\n", rank, size);

	int loop = atoi(argv[1]);
	int number_total_length = BLOCK * loop;

	int *numbers = PSRS(argv[2], number_total_length, rank, size);

	MPI_Finalize();

	delete[] numbers;
	return 0;
}