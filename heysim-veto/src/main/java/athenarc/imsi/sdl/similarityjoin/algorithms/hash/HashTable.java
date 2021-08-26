package athenarc.imsi.sdl.similarityjoin.algorithms.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.googlecode.javaewah.datastructure.BitSet;

import athenarc.imsi.sdl.similarityjoin.Utils;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.RelationMatrix;
import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.SparseVector;

public class HashTable {
    private RelationMatrix matrix = null;
    private HashFamily hashFamily = null;
    private Map<BitSet, List<Integer>> hashTable = null;

    public HashTable(RelationMatrix matrix) {
        this.matrix = matrix;
        this.hashTable = new HashMap<>();
        this.hashFamily = new HashFamily(15, matrix.getColumnsLength());
    }

    public void build(int minValues) {
        for (int i=0; i<this.matrix.getRowsLength(); i++) {
//            int[] row = this.matrix.getExpandedRow(null, i);
            SparseVector row = this.matrix.getRow(i);
            if (row == null) {
                continue;
            }

            // bypass empty row
            if (row.getUsed() < minValues)
                continue;

            BitSet hash = hashFamily.hash(row);
            List<Integer> bucket = this.hashTable.get(hash);

            if (bucket == null) {
                bucket = new ArrayList<Integer>();
                bucket.add(i);
                this.hashTable.put(hash, bucket);
            } else {
                bucket.add(i);
            }
        }
    }

    public List<Integer> probe(int rowIndex, int w) {
        SparseVector row = null;
        try {
            row = this.matrix.getRow(rowIndex);

        // in case the entity that we search is not in the graph
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
        if (row == null) {
            return null;
        }
        BitSet hash = hashFamily.hash(row);

        List<Integer> bucketItems = new ArrayList<>();

        // join bucket items within a hamming distance threshold
        for (Map.Entry<BitSet, List<Integer>> entry : hashTable.entrySet()) {
            if (Utils.hammingDistance(entry.getKey(), hash) <= w) {
                bucketItems.addAll(entry.getValue());
            }
        }

        return bucketItems;
    }

    public Set<BitSet> keySet() {
        return this.hashTable.keySet();
    }

    public Collection<List<Integer>> values() {
        return this.hashTable.values();
    }

    public List<Integer> get(BitSet key) {
        return this.hashTable.get(key);
    }

    public void print() {
        int count = 0;
        for (BitSet set : this.hashTable.keySet()) {
            System.out.println(set.toString());
            System.out.println(this.hashTable.get(set));
            count += this.hashTable.get(set).size();
            System.out.println();
        }
        System.out.println(count);
    }

    public void mergeNearbyBuckets(int w) {

        // list of all bit sets that are keys in the hash table
        List<BitSet> bitVectors = new ArrayList<>(hashTable.keySet());

        // keep track of the bit sets that are already checked
        Set<BitSet> checked = new HashSet<>();

        // check every bit set for its hamming distance with all others
        // bypass if already checked
        for (int i = 0; i < bitVectors.size(); i++) {

            BitSet firstBitVector = bitVectors.get(i);

            if (checked.contains(firstBitVector))
                continue;

            for (int j = i + 1; j < bitVectors.size(); j++) {
                BitSet secondBitVector = bitVectors.get(j);

                if (checked.contains(secondBitVector))
                    continue;

                int distance = Utils.hammingDistance(firstBitVector, secondBitVector);

                // if hamming distance is less than the threshold w set
                // then merge buckets & mark bit sets as checked
                if (distance <= w) {

                    hashTable.get(firstBitVector).addAll(hashTable.get(secondBitVector));
                    hashTable.remove(secondBitVector);

                    checked.add(firstBitVector);
                    checked.add(secondBitVector);
                }
            }
        }
    }
}
