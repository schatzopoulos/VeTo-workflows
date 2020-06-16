from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

import argparse
import time


COO_MATRIX_SCHEMA = StructType([
	StructField('row', LongType()),
	StructField('col', LongType()),
	StructField('val', DoubleType())
])

class SparseMatrix:
	_matrix = None
	_rows = -1
	_cols = -1

	def __init__(self, rows, cols, df):
		self._rows = rows
		self._cols = cols
		self._matrix = df

	def get_sparsity(self):
		return self._matrix.count() / (self._rows * self._cols)
	
	def get_df(self):
		return self._matrix

	def get_rows(self):
		return self._rows

	def get_cols(self):
		return self._cols

	def non_zero(self):
		return self._matrix.count()
	
	def write(self, filename):
		self._matrix.coalesce(1).write.csv(filename, sep='\t')

	def multiply(self, spark, B, enable_broadcast=False):

		self._matrix.createOrReplaceTempView("A")

		if enable_broadcast:
			B = broadcast(B)

		B.get_df().createOrReplaceTempView("B")
		df = spark.sql("""
		SELECT
			A.row row,
			B.col col,
			SUM(A.val * B.val) val
		FROM
			A
		JOIN B ON A.col = B.row
		GROUP BY A.row, B.col
		""")

		return SparseMatrix(self.get_rows(), B.get_cols(), df)
