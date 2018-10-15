package Main;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.*;

public class DocumentFrequencyOrdering {

    public static String[] stopwords = {"a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
    public static Set<String> stopWordSet = new HashSet<>(Arrays.asList(stopwords));


    public static boolean isStopword(String word) {
        if(word.length() < 2) return true;
        if(word.charAt(0) >= '0' && word.charAt(0) <= '9') return true; //remove numbers, "25th", etc
        if(stopWordSet.contains(word)) return true;
        else return false;
    }

    /*Map1 Takes as input the document collection
     * Remove stop words from the documents e.g. a, as, before, the, of
     * Emits the word as key , filename  and count as value
     * */

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        PorterStemmer porterStemmer = new PorterStemmer();
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text fileVal = new Text();

        Hashtable<String, Integer> hash = new Hashtable<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = ((FileSplit) context.getInputSplit());
            String fileName = fileSplit.getPath().getName();
            fileVal.set(fileName);

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            System.out.println(hash);
            while (tokenizer.hasMoreTokens()) {
                String words = porterStemmer.stemWord(tokenizer.nextToken().toLowerCase().replaceAll("[^A-Za-z0-9 ]", ""));
                if(!isStopword(words) ) {
                    word.set(words);
                    context.write(new Text(word), new Text(fileVal + "@" + one));
                }
            }

        }

    }

    /*
     *  Reduce1 outputs the document occurrence of each word in the collection e.g
     *  Reduce1 Takes as input the output of Map1,
     *  Emits as key a word in the collection and as value the number of documents a word occurs
     *  e.g. [excellence 7] , [avenger 10], [cheerful 13]
     *
     * */
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            IntWritable result = new IntWritable();
            int sum = 0;

            List<String> newList = new LinkedList<>();

            for (Text val : values) {
                String tmp = val.toString();

                String [] fileNameAndCount =  tmp.split("@");
                String fileName = fileNameAndCount[0];
                int Count = Integer.parseInt(fileNameAndCount[1]);
                /*
                 *  We add to the newList unique filename ( we eliminate duplicates in the values of a key)
                 *  The size of the newList is therefore the number of documents a word occurs
                 *
                 * */
                if(!(newList.contains(fileName)) && fileName != ""){
                    newList.add(fileName);
                    sum +=Count;
                }
            }
            result.set(sum);

            context.write(key, new Text(String.valueOf(result)));
        }
    }

    /*
     * Map2 receive as input value the output of Reduce1
     * Map2 value is the key of Reduce1 output
     * The groupby mechanism provided by the MapReduce Framework itself is used in this algorithm. The keys are grouped according to the
     * natural order of the key words. Therefore it is reasonable that the document frequency is regarded as the key and the corresponding
     * word as the value
     * */

    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{

        IntWritable val = new IntWritable();
        Text word = new Text();
        @Override
        public void map(LongWritable key, Text values,  Context context) throws IOException, InterruptedException{

            String[] line = values.toString().split("\t");
            word.set(line[0]);
            val = new IntWritable(Integer.parseInt(line[1]));

            context.write(val, word);
        }
    }

    /*
    * The reduce() method emits the value as the key and nothing as the value
    * */
    public static class Reduce2 extends Reducer<IntWritable, Text, Text, NullWritable>{

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text token : values){
                context.write(token, NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Job1");
        job.setJarByClass(DocumentFrequencyOrdering.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        if(success){
            Job job2 = new Job(conf,"Job2");
            job2.setJarByClass(DocumentFrequencyOrdering.class);

            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }


    }
}
