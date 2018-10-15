package Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class DocumentVector {


    //static ReaderClasstest reader = new ReaderClasstest();
    public static class MapVector extends Mapper<LongWritable, Text, Text, Text> {

        private static final IntWritable one = new IntWritable(1);
        private Text fileVal = new Text();

        /*
         * Initialize the hashTable. Each word in the DocumentFrequencyOrdering is given a hash value based on its location
         * in the file. With the first word having a value of 1. So we have a hash table in the form
         * [A,1] [B, 2] [C, 3] [D, 4] ...
         *
         * */
        public Hashtable<String, Integer> hashTableInit() {
            Hashtable<String, Integer> hashtable = new Hashtable<>();
            BufferedReader bufferedReader;
            String inputLine;
            try {
                Path path = new Path("/user/hadoop/docFreqOrder0.0/part-r-00000");
                FileSystem fs = FileSystem.get(new Configuration());
                bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
                int index = 1;
                inputLine = bufferedReader.readLine();
                while (inputLine != null) {
                    //System.out.println(inputLine + "hell0 World");
                    hashtable.put(inputLine, index);
                    inputLine = bufferedReader.readLine();
                    index++;

                }
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
            }
            return hashtable;
        }

        public static String[] stopwords = {"a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
        public static Set<String> stopWordSet = new HashSet<>(Arrays.asList(stopwords));

        public static boolean isStopword(String word) {
            if (word.length() < 2) return true;
            if (word.charAt(0) >= '0' && word.charAt(0) <= '9') return true; //remove numbers, "25th", etc
            if (stopWordSet.contains(word)) return true;
            else return false;
        }

        PorterStemmer porterStemmer = new PorterStemmer();


        /*Takes as input the collection of documents*/
        /*Emits the filename as key and as value a string of form word@result@documentVector*/
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = ((FileSplit) context.getInputSplit());
            String fileName = fileSplit.getPath().getName();
            fileVal.set(fileName);

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {

                String word = porterStemmer.stemWord(tokenizer.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9 ]", ""));
                if (!isStopword(word)) {
                    int hashValue = hashTableInit().get(word);
                    context.write(fileVal, new Text(one + "@" + hashValue));
                }
            }
        }
    }

    public static class ReduceVector extends Reducer<Text, Text, Text, NullWritable>{
        IntWritable result = new IntWritable();

        /*
        * emit as key word@result@documentVector and nothing as value
        * */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int sum = 0;


            List<Integer> LList = new LinkedList<>();
            String docVector = "";
            String tmpText = "";
            for (Text word: values){
                String[] wordValue = word.toString().split("@");
                int hashValue = Integer.parseInt(wordValue[1]);

                /* wordValue[1] is the hashValue of the word in the DocumentFrequency hash table
                *  wordValue[0] is the IntWritable value of 1 for any word in the document

                *  want the hashValue of each word in the document. This hashValue is based on the DocumentFrequency hash table
                *  use a List to store the hashValues with duplicates eliminated
                *
                * */
               if(!(LList.contains(hashValue)) && wordValue[1] != null){
                    LList.add(hashValue);
                    sum += Integer.parseInt(wordValue[0]);
                }

            }

            //sort the list of hashValues with duplicate eliminated
            Collections.sort(LList);
            ListIterator itr = LList.listIterator(0);
            while(itr.hasNext()){
                tmpText = itr.next().toString() +", ";
                if(tmpText != "")
                    docVector += tmpText;
            }
            result.set(sum);
            String vectorText = key+"@"+result+"@"+docVector;
            context.write(new Text(vectorText),NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
      

        Job job = new Job(conf, "Document Vector");

        job.setJarByClass(DocumentVector.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(MapVector.class);
        job.setReducerClass(ReduceVector.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
