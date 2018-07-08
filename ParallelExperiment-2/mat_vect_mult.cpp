#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

void Print_matrix(const char* name, double* mat, int rows, int cols)
{
	printf("The matrix %s\n", name);
	for (int i = 0; i < rows; i++)
	{
		for (int j = 0; j < cols; j++)
			printf("%f ", mat[i*cols + j]);
		printf("\n");
	}
	printf("\n");
}

void Print_vector(const char* name, double* vec, int n)
{
	printf("The vector %s\n", name);
	for (int i = 0; i < n; i++)
		printf("%f ", vec[i]);
	printf("\n");
	printf("\n");
}

void Mat_vect_mult(double *mat, double *vec, double *result, int rows, int cols)
{
	for (int i = 0; i < rows; i++)
	{
		result[i] = 0.0;
		for (int j = 0; j < cols; j++)
			result[i] += mat[i*cols + j] * vec[j];
	}
}

void Gen_Mat(int row, int col, double result[])
{
	int ai = 1;
	for (int i = 0; i < row; i++)
	{
		for (int j = 0; j < col; j++)
		{
			result[i*col + j] = ai++;
		}
	}
}

/*-------------------------------------------------------------------*/
int main(int argc, char *argv[])
{
	if (argc != 3)
	{
		printf("Usage:%s mat_rows mat_cols\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	MPI_Request request;
	int process_count, process_num;
	MPI_Init(nullptr, nullptr);
	MPI_Comm_size(MPI_COMM_WORLD, &process_count);
	MPI_Comm_rank(MPI_COMM_WORLD, &process_num);

	printf("process: %d of %d\n", process_num, process_count);

	double *gen_mat = nullptr;

	int rows = atoi(argv[1]);
	int cols = atoi(argv[2]);

	double *vec = new double[cols * 1];

	// 在主进程生成矩阵和向量
	if (process_num == 0)
	{
		gen_mat = new double[rows * cols];
		Gen_Mat(rows, cols, gen_mat);
		//Print_matrix("gen_mat", gen_mat, rows, cols);

		Gen_Mat(cols, 1, vec);
		//Print_vector("vec", vec, cols * 1);
	}

	MPI_Barrier(MPI_COMM_WORLD);
	double start_time;

	// 发送vec数据到各个进程
	if (process_num == 0)
	{
		start_time = MPI_Wtime();
		for (int i = 1; i < process_count; i++)
			MPI_Isend(vec, cols * 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &request);
	}
	else
	{
		MPI_Recv(vec, cols * 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		//Print_vector("vec", vec, cols * 1);
	}

	// 计算每个进程需要获得矩阵元素的个数及偏移量
	int *sendcounts = new int[process_count];
	int *displs = new int[process_count];
	displs[0] = 0;
	for (int i = 0; i < process_count - 1; i++)
	{
		sendcounts[i] = (rows / process_count) * cols;
		displs[i + 1] = displs[i] + sendcounts[i];
	}
	sendcounts[process_count - 1] = (rows - rows / process_count * (process_count - 1)) * cols;

	// 分发mat数据到各个进程
	double *mat = new double[sendcounts[process_num]];
	MPI_Scatterv(gen_mat, sendcounts, displs, MPI_DOUBLE, mat, sendcounts[process_num], MPI_DOUBLE, 0, MPI_COMM_WORLD);
	int rows_per_process = sendcounts[process_num] / cols;
	//Print_matrix("mat", mat, rows_per_process, cols);
	delete[] gen_mat;

	// 每个进程计算自己的矩阵和向量相乘的结果
	double *result_temp = new double[rows_per_process * 1];
	Mat_vect_mult(mat, vec, result_temp, rows_per_process, cols);
	//Print_vector("result_temp", result_temp, rows_per_process);

	// 主进程收集各个进程的结果
	double *result = nullptr;
	if (process_num == 0)
	{
		result = new double[rows];
	}
	int *recvcounts = new int[process_count];
	for (int i = 0; i < process_count; i++)
	{
		recvcounts[i] = sendcounts[i] / cols;
		displs[i + 1] = displs[i] + recvcounts[i];
	}
	MPI_Gatherv(result_temp, recvcounts[process_num], MPI_DOUBLE, result, recvcounts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);

	if (process_num == 0)
	{
		printf("%.10lfs\n", MPI_Wtime() - start_time);
		//Print_vector("result", result, rows);
	}

	MPI_Finalize();

	delete[] mat;
	delete[] vec;
	delete[] sendcounts;
	delete[] recvcounts;
	delete[] result_temp;
	delete[] result;
	return 0;
}


