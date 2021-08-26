package athenarc.imsi.sdl.similarityjoin.algorithms;

import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.RelationMatrix;
import athenarc.imsi.sdl.similarityjoin.algorithms.topk.TopKQueue;

public abstract class SimilarityJoinAlgorithm {

    private int k;
    private SimilarityMeasure.TYPE similarityMeasure;
    private RelationMatrix relationMatrix = null;

    public final void setK(int k) {
        this.k = k;
    }

    public final void setSimilarityMeasure(SimilarityMeasure.TYPE similarityMeasure) {
        this.similarityMeasure = similarityMeasure;
    }

    public final int getK() {
        return k;
    }

    public final SimilarityMeasure.TYPE getSimilarityMeasure() {
        return similarityMeasure;
    }

    public final void readRelationMatrix(String fileName) {
        this.relationMatrix = new RelationMatrix(false);
        this.relationMatrix.read(fileName);
    }

    public final void printRelationMatrix() {
        relationMatrix.print();
    }

    public final void setRelationMatrix(RelationMatrix matrix) {
        relationMatrix = matrix;
    }

    public final double[][] convertRelationMatrix() {
        return relationMatrix.convert();
    }

    public abstract TopKQueue execute();

    public abstract TopKQueue executeSearch(int rowIndex);

    public abstract long getTopKTime();

    public RelationMatrix getRelationMatrix() {
        return relationMatrix;
    }
}
