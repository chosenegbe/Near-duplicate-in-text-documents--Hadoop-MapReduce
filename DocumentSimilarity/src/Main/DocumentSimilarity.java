package Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class DocumentSimilarity {
   
    private static double t =  0.5; //t is threshold value
    public static class MapDoc extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            * value  is the DocumentVector of the format id@DocumentLength@vectorText, where id is the name of the
            * document, documentLength is the length of the document vector and vectorText is the list of hashvalues
            * of each word in the document arranged in ascending order to ease the computation of the Jaccard similarity
            * */

            // So we split the value based on "@" to get individual component of the DocumentVector
            String[] vectorTextComponents = value.toString().split("@");


            int documentLength = Integer.parseInt(vectorTextComponents[1]);

            /*
            *   PrefixLength = |X| - [t * |X| ] + 1 where |X| is the length of document X and t is the similarity threshold
            * */
            int prefixLength = (int) Math.ceil(documentLength - (t * documentLength)) + 1;

            /*
            * We need to further split the vectorText to get the hashvalue of each word
            * We then split the vectorText based on the "," character
            * */
            String vectorText = vectorTextComponents[2];
            String[] splitHashValues = vectorText.split(",");

            /*
            *  Each of the front prefixLength dimension in the DocumentVector excluding the id and length is respectively
            *  the output key
            *  The output of the Map is of the form
            *  key  value
            *  10   id@length@VectorText
            *  12   id@length@VectorText
            *  5    id@length@VectorText
            *  ..   ...................
            *
            * */
            for (int i = 0; i < prefixLength; i++) {
                context.write(new Text(splitHashValues[i].trim()), new Text(value));
            }
        }
    }
    public static class ReduceDoc extends Reducer<Text, Text, Text, Text> {

        Text SimilarityMeasure = new Text();
        Text VectorKey = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            /*
            * Define three sets, firstVector, secondVector and SetIntersection. 
	      SetIntersection stores the intersection of Set1 and Set2
            *
            * */
            Set<String> firstVector = new LinkedHashSet<>();
            Set<String> secondVector = new LinkedHashSet<>();
            Set<String> SetIntersection = new LinkedHashSet<>();

            Text vectorSetKey = new Text();
            Text docSim = new Text();

            /*
            *  If X have a similarity degree z to Y, then it implies Y has a similarity degree z to X
            *  So X and Y form a form a 2D dimensional matrix of same row and column size, computing the similarity of elements
            *  above or below the leading diagonal is enough. In this way we save computational power.
            *  The MapDoc Class was implemented in a such a way that we can't have  same doc id within a key so a document is never compare to itself.
            *  We define two integers(used as a counter) for each of the foreach statement
            *
            * */

            int outerCounter = 0;
            int innerCounter = 0;

            for (Text v1 : values) {

                String docVectorText1 = v1.toString().trim();
                /* The vectorText is of the form [1, 2, 3, 4, 5, ... ]. I want to remove the commas so that the vectorText
                 * should be of the form [ 1 2 3 4 5 6 ... ] .
                 * */
                String[] vectorTextComponentsV1 = docVectorText1.split("@");
                String idV1 = vectorTextComponentsV1[0].trim();
                String vectorTextV1 = vectorTextComponentsV1[2];

                String hashValues1 = vectorTextV1.replaceAll("[^A-Za-z0-9 ]", "");
                String[] vector1 = hashValues1.split(" ");
                int v1DocLength = Integer.parseInt(vectorTextComponentsV1[1]);
                firstVector.addAll(Arrays.asList(vector1));
                //Iterate the rest of the vectorText of a key

                for (Text v2 : values) {

                    String docVectorText2 = v2.toString().trim();
                    String[] vectorTextComponentsV2 = docVectorText2.split("@");
                    String idV2 = vectorTextComponentsV2[0].trim();
                    String vectorTextV2 = vectorTextComponentsV2[2];
                    String hashValues2 = vectorTextV2.replaceAll("[^A-Za-z0-9 ]", "");
                    String[] vector2 = hashValues2.split(" ");
                    int v2DocLength = Integer.parseInt(vectorTextComponentsV2[1]);
                    

                      //A document is exactly equal to itself , so no need for computation, return 1
                      if(idV1.equals(idV2)){
                            vectorSetKey.set(idV1);
                            docSim.set(" < " + idV2 + "  " + 1.0 + " > ");
                            //tempMap.put(idV1.trim(), " < " + idV2 + "  " + 1.0 + " > ");
                        }
                      else if(innerCounter == outerCounter){ }
                      else if(outerCounter > innerCounter){
                          //We do nothing
                          //We don't have to compute elements above the leading diagonal
                          //The reason being the similarity of X relative to Y is same as Y relative to X
                          }

                        /*
                         *  For a document to be processed, it must passed the length filter.
                         *  If X and Y are two documents of length |X| and |Y| respectively with |X| > |Y|
                         *  then the length filter says |Y| >= t * |X| ,  where t is the similarity threshold (t E (0,1) )
                         * */

                      else{
                              if ((v1DocLength > v2DocLength && v2DocLength > t * v1DocLength)
                                      || (v2DocLength > v1DocLength && v1DocLength > t * v2DocLength) ) {
                                  secondVector.addAll(Arrays.asList(vector2));
                                  SetIntersection.addAll(firstVector);
                                  SetIntersection.retainAll(secondVector);
                                  final int set1 = firstVector.size();
                                  final int set2 = secondVector.size();

                                  final int intersection = SetIntersection.size();

                                  double similarityScore = 1d / (set1 + set2 - intersection) * intersection;
                                  if(similarityScore >= 0.5) {

                                      String docString = "< " + idV2 + " " +similarityScore + " > ";
                                      vectorSetKey.set(idV1);
                                      docSim.set(docString);

				                    }
				                  secondVector.removeAll(secondVector);
                              }
                          }
                      innerCounter++;
                }
                
                innerCounter = 0;
                outerCounter++;
                firstVector.removeAll(firstVector);

            }
            VectorKey.set(vectorSetKey.toString().trim());
            SimilarityMeasure.set(docSim);
            String x = VectorKey.toString().trim();
            if(!x.isEmpty()){
                context.write(VectorKey,SimilarityMeasure);
             }

        }
    }

    public static void main(String[] args) throws Exception {



        Configuration conf = new Configuration();

        Job job = new Job(conf, "Documents Similarity pair");

        job.setJarByClass(DocumentSimilarity.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapDoc.class);
        job.setReducerClass(ReduceDoc.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}
