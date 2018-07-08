#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

double f(double x)
{
	double return_val;

	return_val = x * x;
	return return_val;
}

double Trap(double a, double b, int n, double h)
{
	double integral;
	int k;

	integral = (f(a) + f(b)) / 2.0;
	for (k = 1; k <= n - 1; k++)
	{
		integral += f(a + k * h);
	}
	integral = integral * h;

	return integral;
}

int main(int argc, char *argv[])
{
	if (argc != 4)
	{
		printf("Usage:%s left right slice\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	double l = atof(argv[1]), r = atof(argv[2]);
	int n = atof(argv[3]);
	double h = (r - l) / n;
	double start_time;

	int process_count, process_num;
	MPI_Init(nullptr, nullptr);
	MPI_Comm_size(MPI_COMM_WORLD, &process_count);
	MPI_Comm_rank(MPI_COMM_WORLD, &process_num);

	//printf("process: %d of %d\n", process_num, process_count);
	if (process_num == 0)
	{
		start_time = MPI_Wtime();
	}

	int current_n = n / process_count;
	double start = l + h * current_n * process_num;

	current_n += process_num == process_count - 1 ? n - (n / process_count) * process_count : 0;
	double end = current_n * h + start;
	//printf("%lf, %lf, %d\n", start, end, current_n);

	double integral = Trap(start, end, current_n, h);
	double sum = 0;
	MPI_Reduce(&integral, &sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

	if (process_num == 0)
		printf("sum: %.15lf\n", sum);
	if (process_num == 0)
	{
		printf("%.10lfs\n", MPI_Wtime() - start_time);
		//Print_vector("result", result, rows);
	}

	MPI_Finalize();
	return 0;
}