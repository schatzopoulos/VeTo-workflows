from pyspark.sql.functions import concat, col, lit
from graphframes import GraphFrame
from pyspark.sql.functions import UserDefinedFunction, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from functools import reduce 
import utils
import pagerank
import time

class Graph:
	
	def build(self, spark, metapath, nodes_dir, relations_dir):
		# print("HIN Transformation\t1\tLoading HIN Nodes", flush=True)

		start_time = time.time()
		vertices = self.collect_vertices(spark, metapath, nodes_dir)
		vertices.show(n=5)
		print("--- read vertices %s %s---" % (time.time() - start_time, vertices.rdd.getNumPartitions()))

		print("HIN Transformation\t2\tLoading HIN Edges", flush=True)
		start_time = time.time()
		edges = self.collect_edges(spark, metapath, relations_dir)
		edges.show(n=5)
		print("--- read edges  %s %s ---" % (time.time() - start_time, edges.rdd.getNumPartitions()))

		self._graph = GraphFrame(vertices, edges)

	def collect_vertices(self, spark, metapath, nodes_dir):

		vertices = None
		# print("##### Nodes #####")

		# loop in unique metapath entities
		for v in list(set(metapath)):
			filepath = nodes_dir + v + '.csv'

			# read dataframe from csv file
			df = spark.read.csv(filepath, sep='\t', header=True, inferSchema=True)

			# append entity type to column 'id' and keep rest columns
			name = 'id'
			udf = UserDefinedFunction(lambda x: v + str(x), StringType())
			df2 = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])

			df.unpersist()

			# df2.show(n=50)
			# print(v + " :\t" + str(df2.count()))

			if not vertices:
				vertices = df2
			else:
				# merge dataframes by harmonizing their schema first
				# fill with null non-common columns
				vertices = utils.harmonize_schemas_and_combine(vertices, df2)
				# vertices.show(n=50)

			df2.unpersist()

		# print()

		return vertices

	def collect_edges(self, spark, metapath, relations_dir):
		edges = None

		# print("##### Relations #####")
		for i in range(len(metapath)-1):
			relation = metapath[i:i+2]
			filepath = relations_dir + relation + '.csv'

			# read from csv file using a specific schema
			schema = StructType([
				StructField("src", IntegerType(), False),
				StructField("dst", IntegerType(), False)])

			df = spark.read.csv(filepath, sep='\t', header=False, schema=schema)
			
			# append entity type to to ids of 'src' and 'dst'
		 	# also append type  
			df2 = df.select(
				concat(lit(relation[0]), "src").alias("src"),
				concat(lit(relation[1]), "dst").alias("dst"),
				concat(lit(relation)).alias("type"),
			)
			df.unpersist()

			# print(relation + " :\t" + str(df2.count()))
			
			if not edges:
				edges = df2
			else:
				edges = edges.union(df2)

			df2.unpersist()
			# df2.show(n=10)

		# print()

		return edges

	def transform(self, spark, metapath, constraints, partitions_num):

		motifs = []
		filters = []
		firstEdge = ''
		lastEdge = ''
		# print("HIN Transformation\t3\tExecuting Motif Search", flush=True)

		for i in range(len(metapath)):
			# print(e)
			if (i+1 <= len(metapath)-1):
				relation = metapath[i] + metapath[i+1]

				# keep track of first and edge names
				if firstEdge == '':
					firstEdge = relation.lower()
				lastEdge = relation.lower()

				# add motif
				motifs.append('(' + metapath[i].lower() + str(i) + ')' + '-[' + (relation).lower() + str(i)+ ']->(' + metapath[i+1].lower() + str(i+1) + ')')
				
				# add edge filter based on edge type
				filters.append(relation.lower() + str(i) + ".type = '" + relation + "'")

				# add constraints
				if metapath[i] in constraints:
					filters.append(metapath[i].lower() + str(i) + "." + constraints[metapath[i]].rstrip())

				if metapath[i+1] in constraints:
					filters.append(metapath[i+1].lower() + str(i+1) + "." + constraints[metapath[i+1]].rstrip())
		# concat user-defined filters
		# filters = filters + constraints

		# print(motifs)
		# print(filters)
		# self._graph.vertices.show(n=50)

		# prepare motif query
		motif_query = ';'.join(motifs)
		paths = self._graph.find(motif_query)
		
		start_time = time.time()
		# add filters
		for f in filters:
			paths = paths.filter(f)

		paths = paths.coalesce(partitions_num)
		paths.show(n=5)
		print("--- build paths  %s %s ---" % (time.time() - start_time, paths.rdd.getNumPartitions()))

		start_time = time.time()
		# keep edges of the sub-graph based on the metapath and the constraints given 
		self._subgraph_edges = paths.select(firstEdge + "0.src", lastEdge + str(len(metapath)-2) + ".dst")
		self._subgraph_edges.show(n=5)
		print("--- build subgraph  %s %s ---" % (time.time() - start_time, self._subgraph_edges.rdd.getNumPartitions()))

	def pagerank(self, alpha, tol, partitions_num, outfile):
		start_time = time.time()
		# group edges based on src node
		links = self._subgraph_edges.groupby("src").agg(collect_list("dst"))
		links.show(n=5)
		print("--- build links %s %s ---" % (time.time() - start_time, links.rdd.getNumPartitions()))

		start_time = time.time()
		# transform df to rdd
		links = links.rdd.map(tuple)
		links.take(5)
		print("--- build rdd  %s %s ---" % (time.time() - start_time, links.getNumPartitions()))

		# execute pagerank
		return pagerank.execute(links, alpha, tol, partitions_num, outfile)














