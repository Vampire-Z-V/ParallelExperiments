/* File:    trap.c
 * Purpose: Calculate definite integral using trapezoidal
 *          rule.
 *
 * Input:   a, b, n
 * Output:  Estimate of integral from a to b of f(x)
 *          using n trapezoids.
 *
 * Compile: gcc -g -Wall -o trap trap.c
 * Usage:   ./trap
 *
 * Note:    The function f(x) is hardwired.
 *
 * IPP:     Section 3.2.1 (pp. 94 and ff.) and 5.2 (p. 216)
 */

#include <stdio.h>
#include <omp.h>

#define TIME_START_RECORD \
	double StartTime = omp_get_wtime();
#define TIME_STOP_RECORD \
	double EndTime = omp_get_wtime(); \
	printf("RunTime: %.10fs\n", EndTime - StartTime);

double f(double x);    /* Function we're integrating */
double Trap(double a, double b, int n, double h);

int main(int argc, char *argv[])
{
	double  integral;   /* Store result in integral   */
	double  a, b;       /* Left and right endpoints   */
	int     n;          /* Number of trapezoids       */
	double  h;          /* Height of trapezoids       */

	a = atoi(argv[1]);
	b = atoi(argv[2]);
	n = atoi(argv[3]);


	h = (b - a) / n;

	TIME_START_RECORD
		integral = Trap(a, b, n, h, atoi(argv[4]));
	TIME_STOP_RECORD

	printf("With n = %d trapezoids, our estimate\n", n);
	printf("of the integral from %f to %f = %.15f\n",
		a, b, integral);

	return 0;
}  /* main */

/*------------------------------------------------------------------
 * Function:    Trap
 * Purpose:     Estimate integral from a to b of f using trap rule and
 *              n trapezoids
 * Input args:  a, b, n, h
 * Return val:  Estimate of the integral
 */
double Trap(double a, double b, int n, double h, int thread_num)
{
	double integral= 0;

#pragma omp parallel num_threads(thread_num)
	{
		int current_n = n / thread_num;
		double start = a + h * current_n * omp_get_thread_num();

		current_n += omp_get_thread_num() == thread_num - 1 ? n - (n / thread_num) * thread_num : 0;
		double end = current_n * h + start;

		double temp = (f(start) + f(end)) / 2.0;
		for (int k = 1; k <= current_n -1; k++)
		{
			temp += f(start + k * h);
		}
		temp *= h;

#pragma omp critical
		integral += temp;
	}
	return integral;
}  /* Trap */

/*------------------------------------------------------------------
 * Function:    f
 * Purpose:     Compute value of function to be integrated
 * Input args:  x
 */
double f(double x)
{
	double return_val;

	return_val = x * x;
	return return_val;
}  /* f */
