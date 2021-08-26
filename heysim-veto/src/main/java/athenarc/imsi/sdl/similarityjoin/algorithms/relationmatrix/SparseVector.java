package athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix;

/*
 * Copyright (C) 2003-2006 Bjørn-Ove Heimsund
 *
 * This file is part of MTJ.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or (at your
 * option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

import java.io.Serializable;
import java.util.Arrays;

/**
 * Sparse vector
 */
public class SparseVector implements Serializable {

    /**
     * Data
     */
    double[] data;

    /**
     * Indices to data
     */
    int[] index;

    /**
     * How much has been used
     */
    int used;

    private static final long serialVersionUID = 5L;

    /**
     * Constructor for SparseVector.
     *
     * @param nz
     *            Initial number of non-zeros
     */
    public SparseVector(int nz) {
        data = new double[nz];
        index = new int[nz];
    }

    /**
     * Constructor for SparseVector
     *
     * @param index
     *            Indices of the vector
     * @param data
     *            Entries of the vector
     * @param deep
     *            True for a deep copy. For shallow copies, the given indices
     *            will be used internally
     */
    public SparseVector(int[] index, double[] data, boolean deep) {
        if (index.length != data.length)
            throw new IllegalArgumentException("index.length != data.length");

        sort(index, data);
        used = index.length;

        if (deep) {
            this.index = index.clone();
            this.data = data.clone();
        } else {
            this.index = index;
            this.data = data;
            used = index.length;
        }
    }

    public SparseVector(int[] index, double[] data) {
        this(index, data, false);
    }

    // bubble sort a[] & b[] based on values of a[]
    private void sort(int[] a, double[] b) {
        for (int n = 0; n < a.length; n++) {
            for (int m = 0; m < a.length-1 - n; m++) {
                if (a[m] > a[m + 1]) {
                    int swapString = a[m];
                    a[m] = a[m + 1];
                    a[m + 1] = swapString;
                    double swapInt = b[m];
                    b[m] = b[m + 1];
                    b[m + 1] = swapInt;
                }
            }
        }
    }

    protected void check(int index) {
        if (index < 0)
            throw new IndexOutOfBoundsException("index is negative (" + index
                    + ")");
    }

//    public void set(int index, double value) {
//        check(index);
//
//        // TODO: should we check against zero when setting zeros?
//
//        int i = getIndex(index);
//        data[i] = value;
//        this.index[i] = index;
//    }

    public double get(int index) {
        check(index);

        int in = Arrays.binarySearch(this.index, 0, used, index);
        if (in >= 0)
            return data[in];
        return 0;
    }

    /**
     * Tries to find the index. If it is not found, a reallocation is done, and
     * a new index is returned.
     */
//    private int getIndex(int ind) {
//
//        // Try to find column index
//        int i = Arrays.binarySearch(index, ind, 0, used);
//System.out.println(data.length);
//        System.out.println(used);
//
//        // Found
//        if (i != -1 && i < used && index[i] == ind)
//            return i;
//
//        int[] newIndex = index;
//        double[] newData = data;
//
//        // Check available memory
//        if (used + 1 > data.length) {
//
//            // If zero-length, use new length of 1, else double the bandwidth
//            int newLength = data.length != 0 ? data.length << 1 : 1;
//System.out.println(newLength);
//            // Enforce the maximum size.
//            newLength = Math.min(newLength, this.size);
//
//            // Copy existing data into new arrays
//            newIndex = new int[newLength];
//            newData = new double[newLength];
//
//            System.arraycopy(index, 0, newIndex, 0, used);
//            System.arraycopy(data, 0, newData, 0, used);
//            index = newIndex;
//            data = newData;
//        }
//
//        return ++used;
//        // All ok, make room for insertion
//        System.arraycopy(index, i, newIndex, i + 1, used - i - 1);
//        System.arraycopy(data, i, newData, i + 1, used - i - 1);
//
//        // Put in new structure
//        newIndex[i] = ind;
//        newData[i] = 0.;

        // Update pointers


        // Return insertion index
//        return i;
//    }

    public SparseVector zero() {
        java.util.Arrays.fill(data, 0);
        used = 0;
        return this;
    }

    // dot() is faster but this is more readable
    public double dot2(SparseVector y) {

        // TODO: check for same sized vectors

        double ret = 0;
        for (int i=0, j=0; i<used; i++)
            ret += data[i] * y.get(index[i]);
        return ret;
    }

    // dot product between 2 sparse vectors
    public double dot(SparseVector y) {

        // TODO: check for same sized vectors

        // return zero if one of the vectors has non-zero elements
        int usedY = y.getUsed();
        if (used == 0 || usedY == 0)
            return 0.0;

        double ret = 0.0;
        int[] indexY = y.getIndex();
        double[] dataY = y.getData();

        // compare indexes and sum only if two same indexes are found
        // important: index[] is sorted
        for (int i=0, j=0; i<used && j<usedY; ) {
            if (index[i] < indexY[j])
                i++;
            else if (index[i] > indexY[j])
                j++;
            else {
                ret += data[i] * dataY[j];
                i++;
                j++;
            }
        }
        return ret;
    }

    // dot product with a dense double[] array
    public double dot(double[] y) {

        // TODO: check for same sized vectors

        double ret = 0;
        for (int i = 0; i < used; ++i)
            ret += data[i] * y[index[i]];
        return ret;
    }

    public double norm1() {
        double sum = 0;
        for (int i = 0; i < used; ++i)
            sum += Math.abs(data[i]);
        return sum;
    }

    public double norm2() {
        double norm = 0;
        for (int i = 0; i < used; ++i)
            norm += data[i] * data[i];
        return Math.sqrt(norm);
    }

    /**
     * Returns the internal value array. This array may contain extra elements
     * beyond the number that are used. If it is greater than the number used,
     * the remaining values will be 0. Since this vector can resize its internal
     * data, if it is modified, this array may no longer represent the internal
     * state.
     *
     * @return The internal array of values.
     */
    public double[] getData() {
        return data;
    }

    /**
     * Returns the used indices
     */
    public int[] getUsedIndexes() {
        if (used == index.length)
            return index;

        // could run compact, or return subarray
        // compact();
        int[] indices = new int[used];
        System.arraycopy(index, 0, indices, 0, used);
        return indices;
    }

    /**
     * Gets the raw internal index array. This array may contain extra elements
     * beyond the number that are used. If it is greater than the number used,
     * the remaining indices will be 0. Since this vector can resize its
     * internal data, if it is modified, this array may no longer represent the
     * internal state.
     *
     * @return The internal array of indices, whose length is greater than or
     *         equal to the number of used elements. Indices in the array beyond
     *         the used elements are not valid indices since they are unused.
     */
    public int[] getIndex() {
        return index;
    }

    /**
     * Number of entries used in the sparse structure
     */
    public int getUsed() {
        return used;
    }

    public String toString() {
        String str = "";
        for (int i=0; i<used; i++) {
            str += ("[" + index[i] + "]=" + data[i] + " ");
        }
        return str;
    }
}
