from pyspark import *
from pyspark.sql import *
import sys
import json
from Graph import Graph
import time
import subprocess

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
		alpha = float(config["pr_alpha"]) if ("pr_alpha" in config) else None
		tol = float(config["pr_tol"]) if ("pr_tol" in config) else None
		hin_out = config["hin_out"]
		analysis_out = config["analysis_out"]
		metapath = config["query"]["metapath"]
		constraints = config["query"]["constraints"]


graph = Graph()

# build HIN
graph.build(spark, metapath, nodes_dir, relations_dir, constraints)

# transform HIN to homogeneous network
hgraph = graph.transform(spark)

# when PR params are not given, execute only HIN transformation
if alpha is None and tol is None:
	hgraph.write(hin_out)
	bashCommand = "cd ../spark-distributed-louvain-modularity/dga-graphx/"
	process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()
	print(output)
	print(error)
	bashCommand = "touch edw"
	process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()
else:
	results = graph.pagerank(hgraph, alpha, tol, analysis_out)

