package athenarc.imsi.sdl.similarityjoin.algorithms.lshbased;

import java.util.List;

import athenarc.imsi.sdl.similarityjoin.Utils;
import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityJoinAlgorithm;
import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityMeasure;
import athenarc.imsi.sdl.similarityjoin.algorithms.hash.HashTable;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.RelationMatrix;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.SparseVector;
import athenarc.imsi.sdl.similarityjoin.algorithms.topk.TopKQueue;;

public class PSJoin extends SimilarityJoinAlgorithm {
    private String analysis;
    private int t;      // number of hash tables
    private int w;   // threshold for nearby buckets
    private int minValues;      // min row values threshold for
    private HashTable[] hashTables = null;
    private long topKTime = 0;

    public PSJoin(String analysis, int t, int w, int minValues) {
        this.analysis = analysis;
        this.t = t;
        this.w = w;
        this.minValues = minValues;
        this.hashTables = new HashTable[t];
    }

    @Override
    public TopKQueue execute() {
        Utils.writeProgress(this.analysis, 1, "Reading Relation Matrix");

        RelationMatrix relationMatrix = super.getRelationMatrix();
        
        Utils. writeProgress(this.analysis, 2, "Computing Top-k Results");

        for (int i=0; i<hashTables.length; i++) {
            hashTables[i] = new HashTable(relationMatrix);
            hashTables[i].build(this.minValues);
            // hashTables[i].print();
        }

        return computeTopK(relationMatrix);
    }

    @Override
    public TopKQueue executeSearch(int rowIndex) {
        Utils.writeProgress(this.analysis, 1, "Reading Relation Matrix");

        RelationMatrix relationMatrix = super.getRelationMatrix();

        Utils. writeProgress(this.analysis, 2, "Computing Similar Results");

        for (int i=0; i<hashTables.length; i++) {
            hashTables[i] = new HashTable(relationMatrix);
            hashTables[i].build(this.minValues);
        }

        TopKQueue topK = simSearchToK(relationMatrix, rowIndex);

        return topK;
    }

    private TopKQueue simSearchToK(RelationMatrix relationMatrix, int i) {

        TopKQueue topK = new TopKQueue(super.getK());

        for (HashTable hashTable : hashTables) {

            // find ids in the same bucket
            List<Integer> bucket = hashTable.probe(i, w);
            // System.out.println(bucket);
            if (bucket == null) {
                continue;
            }
            SparseVector row = relationMatrix.getRow(i);

            double normA = row.norm2();
//                    int[] row = m[rowIds.get(i)];
//
//                    double normA = 0.0;
//                    for (int l = 0; l < row.length; l++) {
//                        normA += row[l] * row[l];
//                    }
//                    normA = Math.sqrt(normA);

            for (int j = 0; j < bucket.size(); j++) {

                // bypass the same author
                if (i == bucket.get(j))
                    continue;

                SparseVector innerRow = relationMatrix.getRow(bucket.get(j));
                //                    int[] innerRow = m[rowIds.get(j)];

                double similarity = SimilarityMeasure.calculate(row, innerRow, normA, super.getSimilarityMeasure());
                if (!Double.isNaN(similarity) && similarity > 0) {
                    if (topK.check(similarity)) {
                        topK.add(i, bucket.get(j), similarity);
                    }
                }
            }
        }


        return topK;
    }

    private TopKQueue computeTopK(RelationMatrix relationMatrix) {
        long curTime = System.currentTimeMillis();

        TopKQueue topK = new TopKQueue(super.getK());

        // merge nearby buckets and count total values for progress
        for (HashTable hashTable : hashTables) {
            hashTable.mergeNearbyBuckets(w);
        }

//        System.out.println(countTotalLists);
        for (HashTable hashTable : hashTables) {

//            hashTable.mergeNearbyBuckets(w);

            for (List<Integer> rowIds : hashTable.values()) {

//                System.out.println(count);

                for (int i = 0; i < rowIds.size(); i++) {

                    SparseVector row = relationMatrix.getRow(rowIds.get(i));
                    double normA = row.norm2();
//                    int[] row = m[rowIds.get(i)];
//
//                    double normA = 0.0;
//                    for (int l = 0; l < row.length; l++) {
//                        normA += row[l] * row[l];
//                    }
//                    normA = Math.sqrt(normA);

                    for (int j = i + 1; j < rowIds.size(); j++) {

                        SparseVector innerRow = relationMatrix.getRow(rowIds.get(j));
                        //                    int[] innerRow = m[rowIds.get(j)];

                        double similarity = SimilarityMeasure.calculate(row, innerRow, normA, super.getSimilarityMeasure());
                        if (!Double.isNaN(similarity) && similarity > 0) {
                            if (topK.check(similarity)) {
                                topK.add(rowIds.get(i), rowIds.get(j), similarity);
                            }
                        }
                    }
                }
            }
        }

        topKTime = System.currentTimeMillis() - curTime;

        return topK;
    }

    @Override
    public long getTopKTime() {
        return topKTime;
    }
}
