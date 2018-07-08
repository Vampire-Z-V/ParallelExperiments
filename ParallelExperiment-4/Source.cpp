#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <vector>
#include <omp.h>
using namespace std;

#define BLOCK 1024

//template<typename T>
void DisplayArray(double *_array, int length)
{
	for (int i = 0; i < length; i++)
	{
		printf("%.0lf ", _array[i]);
		//cout << _array[i] << " ";
	}
	printf("\n");
	printf("\n");
	//cout << endl;
}
void DisplayArray(int *_array, int length)
{
	for (int i = 0; i < length; i++)
	{
		printf("%d ", _array[i]);
	}
	printf("\n");
	printf("\n");
}

void PSRS(double *numbers, int n, int p)
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
	// ÿ�����ݵĳ��ȣ����һ�ο��ܻ᲻һ��
	int offset = n / p;
	// Ϊ�����������ռ�
	double *sample = new double[p * p];

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
			sample[sample_start_index++] = numbers[start_index + i * sample_offset];
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
		sample[i] = sample[(i + 1) * p];
	}
#pragma endregion

	// ��ʱ������ս��������
	double *result_temp = new double[n];
	// �����Ԫ���ֺ�ķֽ磬p�飬ÿ��p�������һ��Ϊ�ν�����index
	int *slice_indices = new int[p * p];
	// ��¼ÿ�����ж೤�����ڿ�������
	int *segment_length = new int[p]();

#pragma omp parallel num_threads(p)
	{
#pragma region 6. ��Ԫ����
		int thread_num = omp_get_thread_num();
		int start_index = thread_num * offset;
		int end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		int sample_index = 0;
		const int slice_start_index = thread_num * p;

		// ��ÿһ�����ݰ�����Ԫ���ֳ�С�Σ���¼�ֽ�index
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

#pragma region 7. ȫ�ֽ���
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
				memcpy(result_temp + result_start_index, numbers + copy_start_index, length * sizeof(double));
				result_start_index += length;
			}
		}
		int result_end_index = result_start_index;
		result_start_index = result_start_index_backup;
#pragma omp barrier 
#pragma endregion

#pragma region 8. �鲢����
		vector<int> points(segment_start_indices);
		double min = INT_MAX;
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

double* PSRS(const char* dataFilePath, const int n, const int data_start_offset, const int process_num, const int process_count, const int thread_num, const int curr_round, const int total_round)
{
	MPI_Request request;

	/// 1. ���Ȼ���
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
	double *numbers_in_curr_process = new double[number_length_in_curr_process];
	MPI_File_seek(fh, data_start_offset + sizeof(double) * number_length_in_per_process * process_num, MPI_SEEK_SET);
	MPI_File_read(fh, numbers_in_curr_process, number_length_in_curr_process, MPI_DOUBLE, &status);
	MPI_File_close(&fh);
	//DisplayArray(numbers_in_curr_process, number_length_in_curr_process);

	//double start_time = MPI_Wtime();

	/// 2. �ֲ�����
	//sort(numbers_in_curr_process, numbers_in_curr_process + number_length_in_curr_process);
	PSRS(numbers_in_curr_process, number_length_in_curr_process, thread_num);

	//DisplayArray(numbers_in_curr_process, number_length_in_curr_process);

	/// 3. �������
	double *sample = nullptr;
	double *temp = new double[process_count];

	// ����temp���ڴ�Ÿ��������Լ��Ĳ���������process_count
	int sample_offset = number_length_in_curr_process / process_count;
	for (int i = 0; i < process_count; i++)
	{
		temp[i] = numbers_in_curr_process[i * sample_offset];
	}

	if (process_num == 0)
	{
		// �����̸���Ӹ��������н��ղ���������sample��
		sample = new double[process_count * process_count];
		memcpy(sample, temp, process_count * sizeof(double));

		for (int i = 1; i < process_count; i++)
		{
			MPI_Recv(sample + i * process_count, process_count, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}

		/// 4. ��������
		sort(sample, sample + process_count * process_count);
		//DisplayArray(sample, process_count * process_count);

		/// 5. ѡ����Ԫ
		// ����temp���ڴ����Ԫ���ݣ�����process_count - 1
		for (int i = 0; i < process_count - 1; i++)
		{
			temp[i] = sample[(i + 1) * process_count];
		}

		// ��������������̷�����Ԫ
		for (int i = 1; i < process_count; i++)
		{
			MPI_Isend(temp, process_count - 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
		}
	}
	else
	{
		// ���������ȷ����Լ��Ĳ�����Ȼ��������̴�������Ԫ��Ϣ

		// ����temp��Ÿ��������Լ��Ĳ���������process_count
		MPI_Send(temp, process_count, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
		// ����temp��������̷��͵���Ԫ������process_count - 1
		MPI_Recv(temp, process_count - 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}

	//DisplayArray(temp, process_count - 1);

	/// 6. ��Ԫ����
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

	//DisplayArray(temp, process_count);
	// ����temp��¼ÿ�εĳ��ȣ�����process_count
	for (int i = process_count - 1; i > 0; i--)
	{
		temp[i] -= temp[i - 1];
	}

	// ����Ӧ�εĳ��ȷ��͸���Ӧ�Ľ��̣����Լ��Ĳ�����
	double *receive_data_length = new double[process_count]();
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Isend(temp + i, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
		}
	}

	receive_data_length[process_num] = temp[process_num];
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Recv(receive_data_length + i, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}

	//DisplayArray(temp, process_count);
	//DisplayArray(receive_data_length, process_count);

	/// 7. ȫ�ֽ���
	// �����ݷ�����Ӧ����
	int start_index = 0;
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num && (int)temp[i] != 0)
		{
			MPI_Isend(numbers_in_curr_process + start_index, (int)temp[i], MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
		}
		start_index += (int)temp[i];
	}

	int result_length = 0;
	for (int i = 0; i < process_count; i++)
	{
		result_length += (int)receive_data_length[i];
	}

	double *result_temp = new double[result_length];
	vector<int> segment_start_indices;
	start_index = 0;
	for (int i = 0; i < process_count; i++)
	{
		if ((int)receive_data_length[i] == 0)
			continue;

		if (i != process_num)
		{
			MPI_Recv(result_temp + start_index, (int)receive_data_length[i], MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
		else
		{
			int count = 0;
			for (int j = 0; j < i; j++)
			{
				count += (int)temp[j];
			}
			//printf("%d.........\n", count);
			memcpy(result_temp + start_index, numbers_in_curr_process + count, (int)receive_data_length[i] * sizeof(double));
			//DisplayArray(numbers_in_curr_process + count, (int)receive_data_length[i]);
		}
		segment_start_indices.push_back(start_index);
		start_index += (int)receive_data_length[i];
	}
	delete[] numbers_in_curr_process;
	segment_start_indices.push_back(start_index);

	//DisplayArray(result_temp, result_length);

	/// 8. �鲢����
	vector<int> points(segment_start_indices);
	double min = INT_MAX;
	int *min_index = nullptr;
	int count = 0;
	numbers_in_curr_process = new double[result_length];

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

	//DisplayArray(numbers_in_curr_process, result_length);

	/// ֪ͨ���������Լ���result����
	vector<int> result_length_in_each_process(process_count);
	for (int i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Isend(&result_length, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
	}

	for (int i = 0; i < process_count; i++)
	{
		if (i == process_num)
		{
			result_length_in_each_process[i] = result_length;
		}
		else
		{
			MPI_Recv(&result_length_in_each_process[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}
	//for (auto i : result_length_in_each_process)
	//{
	//	printf("%d ", i);
	//}
	//printf("\n");

	/// Ϊ��ǰ������ϵ����ݽ����������
	sample_offset = n / total_round;
	vector<double> numbers_sample;
	vector<int> result_offsets;

	int *recvcounts = new int[process_count]();
	int *displs = new int[process_count]();
	start_index = 0;
	int end_index = 0;
	for (int i = 0; i < process_count; i++)
	{
		int temp_start_index = end_index;
		result_offsets.push_back(end_index);
		end_index += result_length_in_each_process[i];
		for (; start_index < end_index; start_index += sample_offset)
		{
			recvcounts[i]++;
			displs[i + 1] = displs[i] + recvcounts[i];
			if (i == process_num)
			{
				numbers_sample.push_back(numbers_in_curr_process[start_index - temp_start_index]);
			}
		}
	}

	//DisplayArray(numbers_sample.data(), numbers_sample.size());
	/*DisplayArray(recvcounts, process_count);
	DisplayArray(displs, process_count);*/

	/// �ռ������̵Ĳ���
	double *total_sample = NULL;
	if (process_num == 0)
	{
		total_sample = new double[total_round];
	}

	MPI_Gatherv(numbers_sample.data(), recvcounts[process_num], MPI_DOUBLE, total_sample, recvcounts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);

	/*if (process_num == 0)
	{
		DisplayArray(total_sample, total_round);
	}*/

	/// �������̽��Լ��Ľ��д����ʱ�ļ�
	char temp_file_name[100];
	sprintf(temp_file_name, "temp/data_%d", curr_round);
	MPI_File_open(MPI_COMM_WORLD, temp_file_name, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
	MPI_File_write_at_all(fh, result_offsets[process_num] * sizeof(double), numbers_in_curr_process, result_length, MPI_DOUBLE, &status);
	MPI_File_close(&fh);

	//
	//	printf("%.10lfs", MPI_Wtime() - start_time);
	//	printf("\n");
	//
	//	for (int i = 0; i < result_length - 1; i++)
	//	{
	//		if (numbers_in_curr_process[i] > numbers_in_curr_process[i + 1])
	//		{
	//			printf("sort error");
	//			goto end;
	//		}
	//	}
	//	printf("sort success");
	//	printf("\n");
	//	printf("\n");
	//
	//end:
	delete[] sample;
	delete[] temp;
	delete[] receive_data_length;
	delete[] result_temp;
	delete[] numbers_in_curr_process;
	delete[] recvcounts;
	delete[] displs;
	return total_sample;
}

void PSRS(const char* file_1, const char* file_2, const int file_size, const int load_data_count_per_round, const int round, const int process_num, const int process_count, const int thread_num)
{
	// �൱�ھֲ����򣬽��м������浽�����ϣ�˳�㷵����������Ľ��
	vector<double> sample;
	for (int i = 1; i <= round; i++)
	{
		const char* file = load_data_count_per_round * i * sizeof(double) > file_size ? file_2 : file_1;

		int offset = (i - 1) % (round / 2);
		printf("%d: ", i);
		double *temp_sample = PSRS(file, load_data_count_per_round, load_data_count_per_round * sizeof(double) * offset, process_num, process_count, thread_num, i, round);

		if (process_num == 0)
		{
			for (int i = 0; i < round; i++)
			{
				sample.push_back(temp_sample[i]);
			}
			delete[] temp_sample;
		}

		MPI_Barrier(MPI_COMM_WORLD);
	}

	if (process_num == 0)
	{
		// ��������
		sort(sample.begin(), sample.end());

		for (auto i : sample)
		{
			printf("%.0lf ", i);
		}
		printf("\n");
		printf("\n");

		// ѡ����Ԫ
		for (int i = 0; i < round - 1; i++)
		{
			sample[i] = sample[(i + 1) * round];
		}
		sample.resize(round - 1);

		for (auto i : sample)
		{
			printf("%.0lf ", i);
		}
		printf("\n");
		printf("\n");
	}

	

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
	//ReadTempFile("../../datas/data_double_5k_1_r");
	//ReadTempFile("../../datas/data_double_5k_2_r");
	//ReadTempFile("temp/data_1");
	//ReadTempFile("temp/data_2");
	//ReadTempFile("temp/data_3");
	//ReadTempFile("temp/data_4");
	//ReadTempFile("temp/data_5");
	//ReadTempFile("temp/data_6");
	//ReadTempFile("temp/data_7");
	//ReadTempFile("temp/data_8");

	if (argc != 5)
	{
		printf("Usage:%s data_file_1 data_file_2 each_file_size(M) thread_num\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	int data_file_size = atoi(argv[3]) * BLOCK;
	int round = 8;
	int load_data_count_per_round = data_file_size * 2 / (8 * round); // ÿ�ֶ�ȡdouble�ĸ���

	// ��ʼ��MPI
	int size, rank;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	printf("process: %d of %d\n", rank, size);

	PSRS(argv[1], argv[2], data_file_size, load_data_count_per_round, round, rank, size, atoi(argv[4]));
	//double *numbers = PSRS(argv[2], number_total_length, rank, size, atoi(argv[3]));

	MPI_Finalize();

	//delete[] numbers;
	return 0;
}