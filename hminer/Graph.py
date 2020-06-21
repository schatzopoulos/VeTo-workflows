from pyspark.sql.functions import concat, col, lit, struct, sum, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
from functools import reduce 
import utils
import Pagerank
import time
from SparseMatrix import SparseMatrix
from DynamicOptimizer import DynamicOptimizer
from array import array
import operator
# from graphframes import GraphFrame


class Graph:

	_dimensions = []
	_transition_matrices = []

	def build(self, spark, metapath, nodes_dir, relations_dir, constraints):
		print("HIN Transformation\t1\tLoading HIN Nodes", flush=True)

		# start_time = time.time()
		constraint_ids = self.build_constraint_matrices(spark, metapath, nodes_dir, constraints)

		# vertices.show(n=5)
		# print("--- read vertices %s %s---" % (time.time() - start_time, vertices.rdd.getNumPartitions()))

		print("HIN Transformation\t2\tLoading HIN Edges", flush=True)
		# start_time = time.time()
		self._transition_matrices = self.build_transition_matrices(spark, metapath, relations_dir, constraint_ids)
		# edges.show(n=5)
		# print("--- read edges  %s %s ---" % (time.time() - start_time, edges.rdd.getNumPartitions()))

		# self._graph = GraphFrame(vertices, edges)

	def build_constraint_matrices(self, spark, metapath, nodes_dir, constraints):
		
		vertices = None
		# print("##### Nodes #####")
		dims = {}
		constraint_ids = {}

		# loop in unique metapath entities
		for node in list(metapath):

			# we have already processed dimensions & constraints for this entity
			if node in dims:
				self._dimensions.append(dims[node])
				continue

			# read dataframe from csv file
			df = spark.read.csv(nodes_dir + node + '.csv', sep='\t', header=True, inferSchema=True)
			
			# count number of lines
			count = df.count()
			dims[node] = count
			self._dimensions.append(count)

			if node in constraints:
				df_filtered = df.select("id").where(constraints[node])
				constraint_ids[node] = df_filtered #SparseMatrix(df_filtered)
		
		return constraint_ids

	def build_transition_matrices(self, spark, metapath, relations_dir, constraint_ids):
		transition_matrices = []

		# print("##### Relations #####")
		for i in range(len(metapath)-1):
			relation = metapath[i:i+2]
			# print(relation)

			# read from csv file using a specific schema
			schema = StructType([
				StructField("row", IntegerType(), False),
				StructField("col", IntegerType(), False)])

			relations = spark.read.csv(relations_dir + relation + '.csv', sep='\t', header=False, schema=schema)

			if relation[0] in constraint_ids:
				relations = constraint_ids[relation[0]].join(relations, constraint_ids[relation[0]].id == relations.row).select(relations['*'])

			if relation[1] in constraint_ids:
				relations = relations.join(constraint_ids[relation[1]], relations.col == constraint_ids[relation[1]].id).select(relations['*'])

			transition_matrices.append(SparseMatrix(self._dimensions[i], self._dimensions[i+1], relations.withColumn("val", lit(1))))

		return transition_matrices

	def transform(self, spark):
		print("HIN Transformation\t3\tPreparing Network", flush=True)

		optimizer = DynamicOptimizer()
		optimizer.sparse_optimimal_chain_order(self._dimensions, self._transition_matrices)

		chain_order = []
		optimizer.get_optimal_chain_order(0, len(self._dimensions) - 2, chain_order);
		# print(chain_order)

		temp = []
		tmp_ptr = None

		for (k, l) in chain_order:

			n = len(temp)

			if k >= 0 and l >= 0:

				res = self._transition_matrices[k].multiply(spark, self._transition_matrices[l])
				temp.append(res)

			elif k == -1 and l >= 0:

				temp[n-1] = temp[n-1].multiply(spark, self._transition_matrices[l])

			elif k >= 0 and l == -1:

				temp[n-1] = self._transition_matrices[k].multiply(spark, temp[n-1])

			else:

				temp[n-2] = temp[n-2].multiply(spark, temp[n-1])
				temp.pop()


		return temp[0]

	def pagerank(self, graph, alpha, tol, outfile):

		# aggregate dest nodes based on source and sum number of outgoing edges
		grouped_df = graph.get_df().groupby("row").agg(struct(collect_list("col").alias("cols"), collect_list("val").alias("vals"), sum("val").alias("edges_num")))

		# transform to rdd that is needed for PR
		links = grouped_df.rdd.map(tuple).cache()

		return Pagerank.execute(links, alpha, tol, outfile)

	# def lpa(self, graph, outfile):
	# 	edges = graph.get_df().select(col('row').alias('src'), col('col').alias('dst'))
	# 	vertices = edges.select('src').union(edges.select('dst')).distinct().withColumnRenamed('src', 'id')

	# 	graph = GraphFrame(vertices, edges)
	# 	result = graph.labelPropagation(maxIter=5)
	# 	result.orderBy('label', ascending=True).withColumnRenamed('label', 'Community').write.csv(outfile, sep='\t')
