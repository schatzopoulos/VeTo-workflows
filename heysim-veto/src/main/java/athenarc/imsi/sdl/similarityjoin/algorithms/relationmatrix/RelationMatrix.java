package athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

public class RelationMatrix implements Serializable {

    private SparseVector[] matrix = null;
    // private SparseVector[] impactMatrix = null;
    private boolean includeImpact = false;

    private int columns = 0;
    private static final long serialVersionUID = 5L;

    Map<Integer, Pair<Integer, String>> rowMap = new HashMap<>();

    public RelationMatrix(boolean includeImpact) {
        this.includeImpact = includeImpact;
    }

    public void build(String rowIdsPath, String columnIdsPath) {
//         Graph graph = new Graph();

//         // query for total number of first node type nodes
//         QueryResult result = graph.run(buildCountOfFirstNodeTypeQuery());
//         int rows = result.single().get("count").asInt();

//         this.matrix = new SparseVector[rows];
//         this.impactMatrix = new SparseVector[rows];

//         result = graph.run(buildFirstNodeTypeQuery());

//         // for rows, write neo4j id to index in file
//         PrintWriter writer = openPrintWriter(rowIdsPath);

//         Map<Integer, Integer> idToColumnIndex = new HashMap<>();

//         int row = 0;

//         while (result.hasNext()) {
//             Record record = result.next();

//             int sourceId = record.get("sourceId").asInt();
//             String label = record.get("label").asString();

//             writer.println(sourceId + "\t" + row);
//             rowMap.put(row, new Pair<>(sourceId, label));

//             String joinPathQuery = buildJoinPathQuery(sourceId);
//             QueryResult joinPathResult = graph.run(joinPathQuery);

//             List<Integer> index = new ArrayList<>();
//             List<Double> data = new ArrayList<>();
//             List<Double> impactData = new ArrayList<>();

//             while (joinPathResult.hasNext()) {
//                 record = joinPathResult.next();

//                 int targetId = record.get("targetId").asInt();
//                 double count = record.get("count").asInt();
//                 double pagerank = record.get("pagerank").asDouble();

//                 Integer column = idToColumnIndex.get(targetId);
//                 if (column == null) {
// //                    columnMap.put(columns, new Pair<>(targetId, label2));

//                     idToColumnIndex.put(targetId, columns);
//                     column = columns;
//                     columns++;
//                 }

//                 index.add(column);
//                 data.add(count);
//                 impactData.add(pagerank);
//             }

//             int[] indexArray = index.stream().mapToInt(Integer::intValue).toArray();
//             double[] dataArray = data.stream().mapToDouble(Double::doubleValue).toArray();
//             double[] impactDataArray = impactData.stream().mapToDouble(Double::doubleValue).toArray();

//             this.matrix[row] = new SparseVector(indexArray, dataArray);
//             this.impactMatrix[row] = new SparseVector(indexArray, impactDataArray);

//             if (row % 1000 == 0) {
//                 System.out.println(row);
//             }
//             row++;
//         }

//         writer.close();

//         // write to file neo4j ids to column index
//         writeColumnIdsToFile(idToColumnIndex, columnIdsPath);

//         graph.close();
    }

    public void print() {
        for (int i=0; i<matrix.length; i++) {
            SparseVector v = this.matrix[i];
            if (v != null) {
                System.out.println(i + " -> " + v.toString());
            }
        }
    }

    private int getMaxSrcId(String inputfile) {
        BufferedReader reader;
        int maxId = -1;

		try {
            reader = new BufferedReader(new FileReader(inputfile));

            String line = reader.readLine();
			while (line != null) {
                String[] tokens = line.split("\t");
                int curId = Integer.parseInt(tokens[0]);
                if (curId > maxId) {
                    maxId = curId;
                }

				// read next line
				line = reader.readLine();
			}
            reader.close();
            
		} catch (IOException e) {
			e.printStackTrace();
        }
        return maxId;

    }
    public void read(String inputfile) {
        int rows = this.getMaxSrcId(inputfile) + 1;

        this.matrix = new SparseVector[rows];

        BufferedReader reader;
		try {
            reader = new BufferedReader(new FileReader(inputfile));
            
            List<Integer> index = new ArrayList<>();
            List<Double> data = new ArrayList<>();

            String prevSrcId = "";
            String line = reader.readLine();
			while (line != null) {

                String[] tokens = line.split("\t");

                if (!prevSrcId.equals(tokens[0])) {
                    if (!prevSrcId.isEmpty()) {
                        int[] indexArray = index.stream().mapToInt(Integer::intValue).toArray();
                        double[] dataArray = data.stream().mapToDouble(Double::doubleValue).toArray();
                        int row = Integer.parseInt(prevSrcId);
                        this.matrix[row] = new SparseVector(indexArray, dataArray);
                        // System.out.println(row + " -> " + this.matrix[row]);
                    }
                                
                    index = new ArrayList<>();
                    data = new ArrayList<>();
                    
                    prevSrcId = tokens[0];
                }
                int i = Integer.parseInt(tokens[1]);
                if (i > columns) {
                    this.columns = i;
                }

                index.add(i);
                data.add(Double.parseDouble(tokens[2]));

				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
        }
        this.columns++;
    }

    public SparseVector getRow(int row) {
        // if (this.includeImpact) {
        //     return this.impactMatrix[row];
        // }

        return this.matrix[row];
    }

    public double[] getExpandedRow(double[] arr, int row) {
        if (arr == null) {
            arr = new double[this.getColumnsLength()];
        } else {
            Arrays.fill(arr, 0);
        }

        SparseVector v = this.matrix[row];
        int[] index = v.getIndex();
        double[] data = v.getData();

        for (int i=0; i<v.getUsed(); i++) {
            arr[index[i]] = data[i];
        }
        return arr;
    }

    public double[][] convert() {
        double[][] m = new double[this.getRowsLength()][this.getColumnsLength()];

        for(int i=0; i<this.getRowsLength(); i++) {
            m[i] = this.getExpandedRow(null, i);
        }
        return m;
    }

    public int getRowsLength() {
        return this.matrix.length;
    }

    public int getColumnsLength() {
        return this.columns;
    }

    public Map<Integer, Pair<Integer, String>> getRowMap() {
        return rowMap;
    }

    public void setIncludeImpact(boolean includeImpact) {
        this.includeImpact = includeImpact;
    }
}