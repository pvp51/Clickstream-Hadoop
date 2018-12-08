package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BreadCrumbs {

    //maps the Wikipedia Clickstream input to K(prev) V(curr n) 
	public static class TokenizerMapper extends
        Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws
            IOException, InterruptedException {

            String[] items = value.toString().split("\t");
            String prev = items[0];
            String curr = items[1];
            String type = items[2];
            int n = Integer.parseInt(items[3]);
                
            context.write(new Text(prev), new Text(String.format("%s\t%d",
                curr, n)));
		}
	}

    //reduces set to just the number #1 click for a prev value
    public static class NextClickReducer extends
        Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

            int max = 0;
            String top_click = "";

            for (Text value : values) {
                String[] items = value.toString().split("\t");
                String curr = items[0];
                int n = Integer.parseInt(items[1]);
                if (n > max) {
                    max = n;
                    top_click = curr;
                }
            }
            context.write(key, new Text(top_click));
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Next Click");
		job.setJarByClass(BreadCrumbs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(NextClickReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/next_click"));
		job.waitForCompletion(true);
	}
}
