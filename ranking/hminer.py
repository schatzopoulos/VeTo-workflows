from pyspark import *
from pyspark.sql import *
import sys
import json
from graph import Graph
import time

if len(sys.argv) != 2:
	print("Usage: spark-submit --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 hminer.py config.json", file=sys.stderr)
	sys.exit(-1)

spark = SparkSession.builder.appName('HMiner').getOrCreate()

# supress Spark INFO messages
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

config_file = sys.argv[1]

with open(config_file) as fd:
		config = json.load(fd)
		nodes_dir = config["indir"]
		relations_dir = config["irdir"]
		alpha = float(config["pr_alpha"])
		tol = float(config["pr_tol"])
		partitions_num = int(config["partitions"])	# number of executors * number of cores per executor
		outfile = config["analysis_out"]
		metapath = config["query"]["metapath"]
		constraints = config["query"]["constraints"]


graph = Graph()

# start_time = time.time()
graph.build(spark, metapath, nodes_dir, relations_dir)
# print("--- build graph %s ---" % (time.time() - start_time))

# start_time = time.time()
graph.transform(spark, metapath, constraints)
# print("--- transform %s ---" % (time.time() - start_time))

# start_time = time.time()
results = graph.pagerank(alpha, tol, partitions_num, outfile)
# print("--- pagerank %s ---" % (time.time() - start_time))

