#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <vector>
#include <omp.h>
#include <string.h>
using namespace std;

#define BLOCK 1024 * 1024 * 1024
#define INT_MAX 2147483647 
//template<typename T>
void DisplayArray(double *_array, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%.0lf ", _array[i]);
		//cout << _array[i] << " ";
	}
	printf("\n");
	printf("\n");
	//cout << endl;
}
void DisplayArray(size_t *_array, size_t length)
{
	for (size_t i = 0; i < length; i++)
	{
		printf("%d ", _array[i]);
	}
	printf("\n");
	printf("\n");
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

void PSRS(double *numbers, size_t n, size_t p)
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
	size_t offset = n / p;
	// 为正则采样分配空间
	double *sample = new double[p * p];

#pragma omp parallel num_threads(p)
	{
		size_t thread_num = omp_get_thread_num();
		// 计算出每个线程需要处理的段的起始和终点位置，以及处理的长度
		size_t start_index = thread_num * offset;
		size_t end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;
		size_t length = end_index - start_index;

		sort(numbers + start_index, numbers + end_index);

		// 计算出每个线程存放采样数据的起始位置和采样间隔，每个线程都能获取p个数据
		size_t sample_start_index = thread_num * p;
		size_t sample_offset = length / p;
		for (size_t i = 0; i < p; i++)
		{
			sample[sample_start_index++] = numbers[start_index + i * sample_offset];
		}
	}
#pragma omp barrier 

	sort(sample, sample + p * p);

	// 选择主元，存放在刚刚存放采样数据的前面，节省空间
	for (size_t i = 0; i < p - 1; i++)
	{
		sample[i] = sample[(i + 1) * p];
	}

	// 暂时存放最终结果的数组
	double *result_temp = new double[n];
	// 存放主元划分后的分界，p组，每组p个，最后一个为段结束的index
	size_t *slice_indices = new size_t[p * p];
	// 记录每个段有多长，用于拷贝数据
	size_t *segment_length = new size_t[p]();

#pragma omp parallel num_threads(p)
	{
		size_t thread_num = omp_get_thread_num();
		size_t start_index = thread_num * offset;
		size_t end_index = (thread_num + 1) * offset;
		end_index = thread_num == p - 1 ? n : end_index;

		size_t sample_index = 0;
		const size_t slice_start_index = thread_num * p;

		// 将每一段数据按照主元划分成小段，记录分界index
		for (size_t i = start_index; i < end_index; i++)
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
			for (size_t i = 0; i < p; i++)
			{
				for (size_t j = 0; j < p; j++)
				{
					if (i == 0 && j == 0)
						segment_length[0] += slice_indices[0];
					else
					{
						segment_length[i] += slice_indices[j*p + i] - slice_indices[j*p + i - 1];
					}
				}
			}

			for (size_t i = 1; i < p; i++)
			{
				segment_length[i] += segment_length[i - 1];
			}
		}

#pragma omp barrier 

		size_t result_start_index = thread_num == 0 ? 0 : segment_length[thread_num - 1];
		size_t result_start_index_backup = result_start_index;
		vector<size_t> segment_start_indices;

		for (size_t i = 0; i < p; i++)
		{
			size_t copy_start_index = thread_num == 0 && i == 0 ? 0 : slice_indices[i * p + thread_num - 1];
			size_t copy_end_index = slice_indices[i * p + thread_num];
			size_t length = copy_end_index - copy_start_index;
			if (length != 0)
			{
				segment_start_indices.push_back(result_start_index);
				memcpy(result_temp + result_start_index, numbers + copy_start_index, length * sizeof(double));
				result_start_index += length;
			}
		}
		size_t result_end_index = result_start_index;
		result_start_index = result_start_index_backup;
#pragma omp barrier 

		vector<size_t> points(segment_start_indices);
		double min = INT_MAX;
		size_t *min_index = NULL;
		size_t count = 0;
		size_t length = result_end_index - result_start_index;

		while (count < length)
		{
			size_t i = 0;
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
	}

	delete[] sample;
	delete[] slice_indices;
	delete[] segment_length;
	delete[] result_temp;
}

void PSRS(const char* dataFilePath, const size_t n, const size_t data_start_offset, const size_t process_num, const size_t process_count, const size_t thread_num, const size_t curr_round, const size_t total_round)
{
	MPI_Request request;

	/// 1. 均匀划分
	size_t number_length_in_per_process = n / process_count;
	size_t number_length_in_curr_process = process_num == process_count - 1 ? n - number_length_in_per_process * process_num : number_length_in_per_process;

	// 每个进程读取属于自己的一段数据
	MPI_File fh;
	size_t ierr;
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

	/// 2. 局部排序
	//sort(numbers_in_curr_process, numbers_in_curr_process + number_length_in_curr_process);
	PSRS(numbers_in_curr_process, number_length_in_curr_process, thread_num);

	//DisplayArray(numbers_in_curr_process, number_length_in_curr_process);

	/// 3. 正则采样
	double *sample = NULL;
	double *temp = new double[process_count];

	// 现在temp用于存放各个进程自己的采样，长度process_count
	size_t sample_offset = number_length_in_curr_process / process_count;
	for (size_t i = 0; i < process_count; i++)
	{
		temp[i] = numbers_in_curr_process[i * sample_offset];
	}

	if (process_num == 0)
	{
		// 主进程负责从各个进程中接收采样，存在sample中
		sample = new double[process_count * process_count];
		memcpy(sample, temp, process_count * sizeof(double));

		for (size_t i = 1; i < process_count; i++)
		{
			MPI_Recv(sample + i * process_count, process_count, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}

		/// 4. 采样排序
		sort(sample, sample + process_count * process_count);
		//DisplayArray(sample, process_count * process_count);

		/// 5. 选择主元
		// 现在temp用于存放主元数据，长度process_count - 1
		for (size_t i = 0; i < process_count - 1; i++)
		{
			temp[i] = sample[(i + 1) * process_count];
		}

		// 主进程向各个进程发送主元
		for (size_t i = 1; i < process_count; i++)
		{
			MPI_Isend(temp, process_count - 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
		}
	}
	else
	{
		// 非主进程先发送自己的采样，然后从主进程处接收主元信息

		// 现在temp存放各个进程自己的采样，长度process_count
		MPI_Send(temp, process_count, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
		// 现在temp存放主进程发送的主元，长度process_count - 1
		MPI_Recv(temp, process_count - 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}

	if(process_num == 0)
		DisplayArray(temp, process_count - 1);

	/// 6. 主元划分
	size_t flag_index = 0;
	// 现在temp记录numbers_in_curr_process中第一个比主元大的数的下标，长度process_count
	for (size_t i = 0; i < number_length_in_curr_process; i++)
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

	//DisplayArray(temp, process_count);
	// 现在temp记录每段的长度，长度process_count
	for (size_t i = process_count - 1; i > 0; i--)
	{
		temp[i] -= temp[i - 1];
	}
	//DisplayArray(temp, process_count);

	// 将对应段的长度发送给对应的进程，给自己的不发送
	int *receive_data_length = new int[process_count]();
	for (size_t i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Isend(temp + i, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
		}
	}

	receive_data_length[process_num] = temp[process_num];
	int *displs = new int[process_count]();
	for (size_t i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			double temp;
			MPI_Recv(&temp, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			receive_data_length[i] = temp;
		}
		if (i < process_count - 1)
			displs[i + 1] = displs[i] + (int)receive_data_length[i];
	}

	//DisplayArray(receive_data_length, process_count);
	//DisplayArray(displs, process_count);

	////DisplayArray(temp, process_count);
	////DisplayArray(receive_data_length, process_count);

	/// 7. 全局交换
	size_t result_length = 0;
	for (size_t i = 0; i < process_count; i++)
	{
		result_length += (size_t)receive_data_length[i];
	}
	double *result_temp = new double[result_length];

	// 将数据发到对应进程
	for (size_t i = 0; i < process_count; i++)
	{
		// 计算当前进程发送数据的offset
		size_t offset = 0;
		for (size_t j = 0; j < i; j++)
		{
			offset += temp[j];
		}
		// i进程收，其他进程发
		MPI_Gatherv(numbers_in_curr_process + offset, (int)temp[i], MPI_DOUBLE, result_temp, receive_data_length, displs, MPI_DOUBLE, i, MPI_COMM_WORLD);
	}

	//DisplayArray(result_temp, result_length);

	/// 8. 归并排序
	vector<size_t> points(displs, displs + process_count);
	double min = INT_MAX;
	size_t *min_index = NULL;
	size_t count = 0;
	numbers_in_curr_process = new double[result_length];

	while (count < result_length)
	{
		for (size_t i = 0; i < points.size(); i++)
		{
			int next_index = i == points.size() - 1 ? result_length : displs[i + 1];
			if (points[i] < next_index && result_temp[points[i]] < min)
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

	/// 通知各个进程自己的result长度
	vector<size_t> result_length_in_each_process(process_count);
	for (size_t i = 0; i < process_count; i++)
	{
		if (i != process_num)
		{
			MPI_Isend(&result_length, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
		}
	}

	for (size_t i = 0; i < process_count; i++)
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

	/// 各个进程将自己的结果写入临时文件
	size_t start_index = 0;
	for (size_t i = 0; i < process_num; i++)
	{
		start_index += result_length_in_each_process[i];
	}
	char temp_file_name[100];
	sprintf(temp_file_name, "temp/data_%d", curr_round);
	MPI_File_open(MPI_COMM_WORLD, temp_file_name, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
	MPI_File_write_at_all(fh, start_index * sizeof(double), numbers_in_curr_process, result_length, MPI_DOUBLE, &status);
	MPI_File_close(&fh);

	delete[] sample;
	delete[] temp;
	delete[] receive_data_length;
	delete[] result_temp;
	delete[] displs;
	delete[] numbers_in_curr_process;
}

void PSRS(const char* file_1, const char* file_2, const size_t file_size, const size_t load_data_count_per_round, const size_t round, const size_t process_num, const size_t process_count, const size_t thread_num, const int run_count)
{
	// 相当于局部排序，将中间结果保存到磁盘上，作为归并段
	const char* file = load_data_count_per_round * run_count * sizeof(double) > file_size ? file_2 : file_1;

	size_t offset = (run_count - 1) % (round / 2);
	//printf("%d: ", i);
	PSRS(file, load_data_count_per_round, load_data_count_per_round * sizeof(double) * offset, process_num, process_count, thread_num, run_count, round);
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
	if (argc != 7)
	{
		printf("Usage:%s data_file_1 data_file_2 each_file_size(G) rounds thread_num run_count\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	size_t data_file_size = atoll(argv[3]) * BLOCK;
	size_t round = atoll(argv[4]);
	size_t load_data_count_per_round = data_file_size * 2 / (8 * round); // 每轮读取double的个数

	// 初始化MPI
	int size, rank;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	printf("process: %d of %d\n", rank, size);
	double start_time = MPI_Wtime();
	PSRS(argv[1], argv[2], data_file_size, load_data_count_per_round, round, rank, size, atoi(argv[5]), atoi(argv[6]));
	start_time = MPI_Wtime() - start_time;
	double delta_time;
	MPI_Reduce(&start_time, &delta_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
	if(rank == 0)
		printf("%.15lfs\n\n", delta_time);

	MPI_Finalize();
	return 0;
}