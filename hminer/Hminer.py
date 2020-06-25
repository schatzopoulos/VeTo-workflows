from pyspark import *
from pyspark.sql import *
import sys
import json
from Graph import Graph
import time
from pyspark.sql.functions import col

if len(sys.argv) != 2:
	print("Usage: spark-submit hminer.py config.json", file=sys.stderr)
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
		analyses = config["analyses"]
		alpha = float(config["pr_alpha"]) if ("pr_alpha" in config) else None
		tol = float(config["pr_tol"]) if ("pr_tol" in config) else None
		edgesThreshold = int(config["edgesThreshold"])
		hin_out = config["hin_out"]
		join_hin_out = config["join_hin_out"]
		ranking_out = config["ranking_out"]
		communities_out = config["communities_out"]
		metapath = config["query"]["metapath"]
		joinpath = config["query"]["joinpath"]
		constraints = config["query"]["constraints"]

printLogs = True
if "Ranking" in analyses or "Community Detection" in analyses:
	
	# In ranking and community detection a homegeneous graph is needed 
	graph = Graph()
	graph.build(spark, metapath, nodes_dir, relations_dir, constraints, printLogs)
	hgraph = graph.transform(spark, printLogs)
	
	hgraph.filter(col("val") >=  edgesThreshold)
	edges_count = hgraph.non_zero()
	
	# abort when resulted network contains no edges or is huge
	if edges_count == 0:
		sys.exit(100)
	elif edges_count >= 5000000:
		sys.exit(200)

	if "Ranking" in analyses:
		graph.pagerank(hgraph, alpha, tol, ranking_out)

	if "Community Detection" in analyses:
		hgraph.write(hin_out)
		# graph.lpa(hgraph, communities_out)

	printLogs = False

# the graph differs in case of similarity search & join
if "Similarity Join" in analyses or "Similarity Search" in analyses:
	graph = Graph()
	graph.build(spark, joinpath, nodes_dir, relations_dir, constraints, printLogs)
	hgraph = graph.transform(spark, printLogs)
	
	hgraph.sort()	# similarity join & search code assumes sorted input graph

	hgraph.write(join_hin_out)