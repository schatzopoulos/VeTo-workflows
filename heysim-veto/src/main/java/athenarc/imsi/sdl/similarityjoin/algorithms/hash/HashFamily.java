package athenarc.imsi.sdl.similarityjoin.algorithms.hash;

import java.util.Random;

import com.googlecode.javaewah.datastructure.BitSet;

import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.SparseVector;

public class HashFamily {
    private int r;      // number of hash functions
    private int d;      // dimension of each hash function
    private double[][] hashFunctions = null;

    public HashFamily(int r, int d) {
        this.r = r;
        this.d = d;

        Random rand = new Random();

        this.hashFunctions = new double[r][];
        for (int i=0; i<this.hashFunctions.length; i++) {
            this.hashFunctions[i] = new double[d];
            for (int j=0; j<this.hashFunctions[i].length; j++){
                this.hashFunctions[i][j] = rand.nextGaussian();

            }
        }
    }

    public void print() {
        for (int i=0; i< this.hashFunctions.length; i++) {
            for (int j=0; j< this.hashFunctions[i].length; j++) {
                System.out.print(this.hashFunctions[i][j] + " ");
            }
            System.out.println();
        }
    }

    public BitSet hash(SparseVector v) {
        BitSet hashCode = new BitSet(this.r);

        for (int i=0; i<this.hashFunctions.length; i++) {
            if (v.dot(this.hashFunctions[i]) >= 0) {
                hashCode.set(i);
            }
        }
        return hashCode;
    }
}
