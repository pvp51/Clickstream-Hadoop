package hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	private static final String INTERMEDIATE = "/data/intermediate";

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text fname = new Text();
		private List<String> uniqueWords = Arrays.asList("education", "politics", "sports", "agriculture");

		@Override
		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
			fname.set(((FileSplit) context.getInputSplit()).getPath().toString());
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

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

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Map<String, Integer> wordCountMap = new HashMap<String, Integer>();

			for (Text val : values) {
				String presentWord = val.toString();
				if (!wordCountMap.containsKey(presentWord)) {
					wordCountMap.put(presentWord, 1);
				} else {
					wordCountMap.put(presentWord, wordCountMap.get(presentWord) + 1);
				}
			}

			int max = 0;
			String mostFreqWord = null;
			for (String uniqueWord : wordCountMap.keySet()) {
				if (wordCountMap.get(uniqueWord) > max) {
					max = wordCountMap.get(uniqueWord);
					mostFreqWord = uniqueWord;
				}
			}
			context.write(key, new Text(mostFreqWord));
		}
	}

	public static class Mapping extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				itr.nextToken();
				Text word = new Text(itr.nextToken());
				context.write(word, new IntWritable(1));
			}
		}
	}

	public static class Reducing extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Job 1");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE));
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Job 2");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(Mapping.class);
		job2.setCombinerClass(Reducing.class);
		job2.setReducerClass(Reducing.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}