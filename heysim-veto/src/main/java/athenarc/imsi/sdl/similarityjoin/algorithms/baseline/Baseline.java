package athenarc.imsi.sdl.similarityjoin.algorithms.baseline;

import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityJoinAlgorithm;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.RelationMatrix;
import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityMeasure;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.SparseVector;
import athenarc.imsi.sdl.similarityjoin.algorithms.topk.TopKQueue;

public class Baseline extends SimilarityJoinAlgorithm {

    private long topKTime = 0;
    private int minValues = -1;
    private double simThreshold = -1;

    public Baseline(double simThreshold, int minValues) {
        this.simThreshold = simThreshold;
        this.minValues = minValues;
    }

    @Override
    public TopKQueue execute() {
//        int[][] matrix = super.convertRelationMatrix();

        TopKQueue topK = computeTopK(super.getRelationMatrix());

        return topK;
    }

    @Override
    public TopKQueue executeSearch(int authorId) {

        RelationMatrix matrix = super.getRelationMatrix();
        SparseVector authorRow = matrix.getRow(authorId);
        if (authorRow == null)
            return null;

        double authorNorm = authorRow.norm2();

        for (int i=0; i<matrix.getRowsLength(); i++) {

            SparseVector currentRow = matrix.getRow(i);

            // bypass rows with less than minValues
            if (currentRow == null
                    || authorId == i
                    || currentRow.getUsed() < this.minValues)
                continue;

            double similarity = SimilarityMeasure.calculate(authorRow, currentRow, authorNorm, super.getSimilarityMeasure());

            if (!Double.isNaN(similarity) && similarity >= this.simThreshold) {
                System.out.println(authorId + "\t" + i + "\t" + similarity);
            }
        }

        return null;
    }

    private TopKQueue computeTopK(RelationMatrix relationMatrix) {
        long curTime = System.currentTimeMillis();
        TopKQueue topK = new TopKQueue(super.getK());

        for (int i=0; i<relationMatrix.getRowsLength(); i++) {
            SparseVector row = relationMatrix.getRow(i);
            double rowNorm = row.norm2();
//            int[] row = m[i];
//            double rowNorm = 0.0;
//            for (int l=0; l<row.length; l++) {
//                rowNorm += row[l] * row[l];
//            }
//            rowNorm = Math.sqrt(rowNorm);

            for (int j=i+1; j<relationMatrix.getRowsLength(); j++) {
                SparseVector innerRow = relationMatrix.getRow(j);
//                int[] innerRow = m[j];
//
                double similarity = SimilarityMeasure.calculate(row, innerRow, rowNorm, super.getSimilarityMeasure());
//            System.out.println(similarity);
                if (!Double.isNaN(similarity)) {
                    if (topK.check(similarity)) {
                        topK.add(i, j, similarity);
                    }
                }
            }
//            break;
        }

        topKTime = System.currentTimeMillis() - curTime;

        return topK;
    }

    public long getTopKTime() {
        return topKTime;
    }
}
