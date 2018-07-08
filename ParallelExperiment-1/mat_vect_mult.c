/* File:     mat_vect_mult.c
*
* Purpose:  Implement serial matrix-vector multiplication using
*           one-dimensional arrays to store the vectors and the
*           matrix.
*
* Compile:  gcc -g -Wall -o mat_vect_mult mat_vect_mult.c
* Run:      ./mat_vect_mult
*
* Input:    Dimensions of the matrix (m = number of rows, n
*              = number of columns)
*           n-dimensional vector x
* Output:   Product vector y = Ax
*
* Errors:   if the number of user-input rows or column isn't
*           positive, the program prints a message and quits.
* Note:     Define DEBUG for verbose output
*
* IPP:      Section 3.4.9 (pp. 113 and ff.), Section 4.3 (pp. 159
*           and ff.), and Section 5.9 (pp. 252 and ff.)
*/
#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

void Get_dims(int* m_p, int* n_p);
void Read_matrix(const char prompt[], double A[], int m, int n);
void Read_vector(const char prompt[], double x[], int n);
void Print_matrix(const char title[], double A[], int m, int n);
void Print_vector(const char title[], double y[], int m);
void Mat_vect_mult(double A[], double x[], double y[], int m, int n, int thread_num);
void Gen_Mat(int row, int col, double result[]);

/*-------------------------------------------------------------------*/
int main(int argc, char *argv[])
{
	double* A = NULL;
	double* x = NULL;
	double* y = NULL;
	int m, n;

	Get_dims(&m, &n);
	A = (double*)malloc(m*n * sizeof(double));
	x = (double*)malloc(n * sizeof(double));
	y = (double*)malloc(m * sizeof(double));
	if (A == NULL || x == NULL || y == NULL)
	{
		fprintf(stderr, "Can't allocate storage\n");
		exit(-1);
	}

	Gen_Mat(m, n, A);
	Gen_Mat(n, 1, x);

	double StartTime = omp_get_wtime();
	Mat_vect_mult(A, x, y, m, n, atoi(argv[1]));
	double EndTime = omp_get_wtime();

	printf("RunTime: %.10fs\n", EndTime - StartTime);
	free(A);
	free(x);
	free(y);
	return 0;
}  /* main */


   /*-------------------------------------------------------------------
   * Function:   Get_dims
   * Purpose:    Read the dimensions of the matrix from stdin
   * Out args:   m_p:  number of rows
   *             n_p:  number of cols
   *
   * Errors:     If one of the dimensions isn't positive, the program
   *             prints an error and quits
   */
void Get_dims(
	int*  m_p  /* out */,
	int*  n_p  /* out */
)
{
	printf("Enter the number of rows\n");
	scanf("%d", m_p);
	printf("Enter the number of columns\n");
	scanf("%d", n_p);

	if (*m_p <= 0 || *n_p <= 0)
	{
		fprintf(stderr, "m and n must be positive\n");
		exit(-1);
	}
}  /* Get_dims */

   /*-------------------------------------------------------------------
   * Function:   Read_matrix
   * Purpose:    Read the contents of the matrix from stdin
   * In args:    prompt:  description of matrix
   *             m:       number of rows
   *             n:       number of cols
   * Out arg:    A:       the matrix
   */
void Read_matrix(
	const char  prompt[]  /* in  */,
	double		A[]       /* out */,
	int			m         /* in  */,
	int			n         /* in  */
)
{
	int i, j;

	printf("Enter the matrix %s\n", prompt);
	for (i = 0; i < m; i++)
		for (j = 0; j < n; j++)
			scanf("%lf", &A[i*n + j]);
}  /* Read_matrix */

   /*-------------------------------------------------------------------
   * Function:   Read_matrix
   * Purpose:    Read a vector from stdin
   * In args:    prompt:  description of matrix
   *             n:       order of matrix
   * Out arg:    x:       the vector being read in
   */
void Read_vector(
	const char  prompt[]  /* in  */,
	double		x[]       /* out */,
	int			n         /* in  */
)
{
	int i;

	printf("Enter the vector %s\n", prompt);
	for (i = 0; i < n; i++)
		scanf("%lf", &x[i]);
}  /* Read_vector */


   /*-------------------------------------------------------------------
   * Function:   Print_matrix
   * Purpose:    Print a matrix to stdout
   * In args:    title:  title for output
   *             A:      the matrix
   *             m:      number of rows
   *             n:      number of cols
   */
void Print_matrix(
	const char  title[]  /* in */,
	double		A[]      /* in */,
	int			m        /* in */,
	int			n        /* in */
)
{
	int i, j;

	printf("\nThe matrix %s\n", title);
	for (i = 0; i < m; i++)
	{
		for (j = 0; j < n; j++)
			printf("%f ", A[i*n + j]);
		printf("\n");
	}
}  /* Print_matrix */

   /*-------------------------------------------------------------------
   * Function:   Print_vector
   * Purpose:    Print the contents of a vector to stdout
   * In args:    title:  title for output
   *             y:      the vector to be printed
   *             m:      the number of elements in the vector
   */
void Print_vector(
	const char  title[]  /* in */,
	double		y[]      /* in */,
	int			m        /* in */
)
{
	int i;

	printf("\nThe vector %s\n", title);
	for (i = 0; i < m; i++)
		printf("%f ", y[i]);
	printf("\n");
}  /* Print_vector */


/*-------------------------------------------------------------------
* Function:   Mat_vect_mult
* Purpose:    Multiply a matrix by a vector
* In args:    A: the matrix
*             x: the vector being multiplied by A
*             m: the number of rows in A and components in y
*             n: the number of columns in A components in x
* Out args:   y: the product vector Ax
*/
void Mat_vect_mult(
	double  A[]  /* in  */,
	double  x[]  /* in  */,
	double  y[]  /* out */,
	int     m    /* in  */,
	int     n    /* in  */,
	int thread_num
)
{
	int i, j;

#pragma omp parallel for num_threads(thread_num)
	for (i = 0; i < m; i++)
	{
		y[i] = 0.0;
		for (j = 0; j < n; j++)
			y[i] += A[i*n + j] * x[j];
	}
}  /* Mat_vect_mult */

void Gen_Mat(int row, int col, double result[])
{
	int i, j;
	int ai = 0;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			result[i*col + j] = ai++;
		}
	}
}
