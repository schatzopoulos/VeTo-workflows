package athenarc.imsi.sdl.similarityjoin.algorithms;

import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.SparseVector;

public class SimilarityMeasure {
    public enum TYPE { PATH_SIM, JOIN_SIM };

    public static double calculate(SparseVector v1, SparseVector v2, double v1Norm, TYPE measure) {
        switch (measure) {
            case JOIN_SIM:
                return joinSim(v1, v2, v1Norm);
            case PATH_SIM:
                throw new UnsupportedOperationException();
            default:
                return -1;
        }
    }

    public static double calculate(int[] v1, int[] v2, double v1Norm, TYPE measure) {
        switch (measure) {
            case JOIN_SIM:
                return joinSim(v1, v2, v1Norm);
            case PATH_SIM:
                throw new UnsupportedOperationException();
            default:
                return -1;
        }
    }

    private static double joinSim(SparseVector v1, SparseVector v2, double v1Norm) {
        return (v1.dot(v2) / (v1Norm * v2.norm2()));
    }

    private static double joinSim(int[] v1, int[] v2, double v1Norm) {
        double dotProduct = 0.0, secondNorm = 0.0;

        for (int i=0; i<v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            secondNorm += v2[i] * v2[i];
        }

        return ((dotProduct) / (v1Norm * Math.sqrt(secondNorm)));
    }
}
