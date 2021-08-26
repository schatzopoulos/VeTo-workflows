package athenarc.imsi.sdl.similarityjoin.algorithms.topk;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.bson.Document;
import org.javatuples.Pair;
import org.springframework.web.bind.annotation.GetMapping;

import athenarc.imsi.sdl.similarityjoin.algorithms.relationmatrix.RelationMatrix;

public class TopKQueue {
    private class TopKQueueRecord implements Comparable<TopKQueueRecord> {
        private Pair<Integer, Integer> pair;
        private double similarity;

        public TopKQueueRecord(Pair<Integer, Integer> p, double similarity) {
            this.pair = p;
            this.similarity = similarity;
        }

        @Override
        public int compareTo(TopKQueueRecord that) {
            if(this.similarity < that.getSimilarity())
                return -1;
            else if(this.similarity > that.getSimilarity())
                return 1;
            return 0;
        }

        public Pair<Integer, Integer> getPair() {
            return pair;
        }

        private double getSimilarity() {
            return (this.similarity > 1) ? 1 : this.similarity;
        }

        @Override
        public String toString() {
            return "(" + this.pair.toString() + " -> " + this.similarity + ")";
        }
    }

    private PriorityQueue<TopKQueueRecord> heap = null;
    private int k = 0;

    public TopKQueue(int k) {
        this.k = k;
        this.heap = new PriorityQueue<TopKQueueRecord>(k);
    }

    public boolean check(double similarity) {
        return ((!Double.isNaN(similarity))
            && ((heap.size() < k) || (!heap.isEmpty() && (similarity > heap.peek().getSimilarity()))));
    }

    public void add(int val1, int val2, double similarity) {
        if (heap.size() == this.k) {
            heap.remove(heap.peek());
        }
        heap.offer(new TopKQueueRecord(new Pair<Integer, Integer>(val1, val2), similarity));
    }

    public void write(String outfile) throws IOException {
        FileWriter fileWriter = new FileWriter(outfile);
        PrintWriter printWriter = new PrintWriter(fileWriter);
    
        TopKQueueRecord[] topkArr = new TopKQueueRecord[heap.size()];
        Arrays.sort(this.heap.toArray(topkArr), Collections.reverseOrder());

        for (TopKQueueRecord record : topkArr) {
            Pair<Integer, Integer> p = record.getPair();
            printWriter.print(p.getValue0() + "\t" +p.getValue1() + "\t"+ record.getSimilarity() + "\n");
        }
        printWriter.close();
    }

    public void print() {
        Object[] arr = this.heap.toArray();
        System.out.println(Arrays.toString(arr));
    }

    @GetMapping
    public List<Document> toJson(RelationMatrix matrix) {
        List<Document> arr = new ArrayList<>();

        Map<Integer, Pair<Integer, String>> rowMap = matrix.getRowMap();
        TopKQueueRecord[] topkArr = new TopKQueueRecord[heap.size()];
        Arrays.sort(this.heap.toArray(topkArr), Collections.reverseOrder());

        for (TopKQueueRecord r : topkArr) {
            Document doc = new Document();
            Pair<Integer, Integer> p = r.getPair();

            Document s = new Document();
            Pair<Integer, String> pair = rowMap.get(p.getValue0());
            s.append("id", pair.getValue0());
            s.append("label", pair.getValue1());
            doc.append("s", s);

            Document t = new Document();
            pair = rowMap.get(p.getValue1());
            t.append("id", pair.getValue0());
            t.append("label", pair.getValue1());
            doc.append("t", t);
            doc.append("score", r.getSimilarity());

            arr.add(doc);
        }

        return arr;
    }

}
