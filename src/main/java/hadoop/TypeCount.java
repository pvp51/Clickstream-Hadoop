package hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TypeCount {

	//maps the Wikipedia Clickstream input to K(prev) V(curr n) 
	public static class TokenizerMapper extends
	Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws
		IOException, InterruptedException {

			String[] items = value.toString().split("\t");
			String type = items[2];
			context.write(new Text(type), new Text(type));
		}
	}

	//reduces set to just the number #1 click for a prev value
	public static class TyepCountReducer extends
	Reducer<Text, Text, Text, Text> {
		Map<String, Integer> typeCount = new HashMap<>();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			int recordCount = 0;
			String count = "";
			
			for(@SuppressWarnings("unused") Text val : values){
				recordCount++;
			}
			count = ""+recordCount;
			context.write(key, new Text(count));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Type Count");
		job.setJarByClass(NextClick.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(TyepCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/type_count"));
		job.waitForCompletion(true);

	}

}
