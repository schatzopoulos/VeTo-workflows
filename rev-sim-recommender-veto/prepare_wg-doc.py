import sys
import csv
import os 

def jaccard_similarity(s1, s2):
	return float(len(s1.intersection(s2)) / len(s1.union(s2)))

# Define CSV dialect to be used.
csv.register_dialect(
	'exp_dialect',
	delimiter = '\t'
)

dict = {}

# build author vectors
with open(sys.argv[1]) as fd:
	records = csv.reader(fd, dialect='exp_dialect')
	for rec in records:
		if rec[0] not in dict:
			dict[rec[0]] = set()
			
		dict[rec[0]].add(rec[1])
fd.close()	

# print(dict)
# 
with open(sys.argv[2]) as fd2:
	authors = csv.reader(fd2, dialect='exp_dialect')
	for author in authors:
		author = author[0]
		if author not in dict:
			continue

		author_vec = dict[author]
		similarities = []
		for authorB in dict.keys():
			if author == authorB:
				continue

			authorB_vec = dict[authorB]
			sim = jaccard_similarity(author_vec, authorB_vec)
			if (sim > 0.05):
				similarities.append((authorB, sim))

		similarities = sorted(similarities, key = lambda x: x[1], reverse=True)
		for sim_tuple in similarities[:10000]:
				print(str(author) + "\t" + str(sim_tuple[0]) + "\t" + str(sim_tuple[1]))	

fd2.close()

