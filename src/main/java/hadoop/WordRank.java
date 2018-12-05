package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class WordRank {

    private static final String INTERMEDIATE = "/data/intermediate";

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text fname = new Text();

        private List<String> uniqueWords = Arrays.asList("education", "politics", "sports", "agriculture");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            fname.set(filePath.substring(filePath.lastIndexOf('/') + 1));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (uniqueWords.contains(word.toString())) {
                    context.write(fname, word);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<String, Integer> wordCountMap = new HashMap<String, Integer>();

            for (Text value : values) {
                String presentWord = value.toString();
                if (!wordCountMap.containsKey(presentWord)) {
                    wordCountMap.put(presentWord, 1);
                } else {
                    wordCountMap.put(presentWord, wordCountMap.get(presentWord) + 1);
                }
            }

            Map<String, Integer> sortMap = new LinkedHashMap<String, Integer>();

//            wordCountMap.entrySet().stream()
//                    .sorted((Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
//                            -> o2.getValue().compareTo(o1.getValue()))
//                    .forEachOrdered(x -> sortMap.put(x.getKey(), x.getValue()));

            String wordRank = "";
            for (String uniqueWord : sortMap.keySet()) {
                wordRank = wordRank + uniqueWord + "-";
            }
            wordRank = wordRank.substring(0, wordRank.length() - 1);
            context.write(key, new Text(wordRank));
        }
    }

    public static class Mapping extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                Text state = new Text(itr.nextToken());
                Text ranking = new Text(itr.nextToken());
                context.write(ranking, state);
            }
        }
    }

    public static class Reducing extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context
            ) throws IOException, InterruptedException {
            String states = "";
            for (Text val : values) {
                states += val + ", ";
            }
            states = states.substring(0, states.length() - 2);
            context.write(key, new Text(states));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(WordRank.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE));
        job.waitForCompletion(true);

        // Job 2
        Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJarByClass(WordRank.class);
        job2.setMapperClass(Mapping.class);
        job2.setCombinerClass(Reducing.class);
        job2.setReducerClass(Reducing.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
