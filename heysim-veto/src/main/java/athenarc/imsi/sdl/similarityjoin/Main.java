package athenarc.imsi.sdl.similarityjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import athenarc.imsi.sdl.similarityjoin.algorithms.baseline.Baseline;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityJoinAlgorithm;
import athenarc.imsi.sdl.similarityjoin.algorithms.SimilarityMeasure;
import athenarc.imsi.sdl.similarityjoin.algorithms.lshbased.PSJoin;
import athenarc.imsi.sdl.similarityjoin.algorithms.topk.TopKQueue;

public class Main {
    public static void main(String [] args) {

        // check params
        if (args.length != 4) {
            System.out.println("Usage: java -jar EntitySimilarity.jar <hin_in> <author_ids> <min_values> <similarityThreshold>");
            System.exit(1);
        }


        String hinInput = (String) args[0];
        String authorIdsFile = (String) args[1];
        int minValues = Integer.parseInt(args[2]);
        double simThreshold = Double.parseDouble(args[3]);

        SimilarityJoinAlgorithm algorithm = new Baseline(simThreshold, minValues);
        algorithm.setSimilarityMeasure(SimilarityMeasure.TYPE.JOIN_SIM);
        algorithm.readRelationMatrix(hinInput);


        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(authorIdsFile));
            String line = reader.readLine();
            while (line != null) {
//                System.out.println(line);

                algorithm.executeSearch(Integer.parseInt(line));

                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
