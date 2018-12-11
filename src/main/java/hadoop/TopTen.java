package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTen {

    //maps the Wikipedia Clickstream input to K(type curr) V(n) 
	public static class TokenizerMapper extends
        Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws
            IOException, InterruptedException {

            String[] items = value.toString().split("\t");
            String curr = items[1];
            String type = items[2];
            int n = Integer.parseInt(items[3]);
                
            context.write(new Text(type + "\t" + curr), new IntWritable(n));
		}
	}

    //reduces set to the sum of instances for a type and curr link 
    public static class SumReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
           
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }

    //maps the output from part 1 to K(type) V(link n) 
	public static class OutputMapper extends
        Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws
            IOException, InterruptedException {

            String[] items = value.toString().split("\t");
            String type = items[0];
            String link = items[1];
            int n = Integer.parseInt(items[2]);
                
            context.write(new Text(type), new Text(link + "\t" + n));
		}
	}

    //keeps track of a link and n
    private static class Pair {
        String link;
        int n;

        Pair(String link, int n) {
            this.link = link;
            this.n = n;
        }
    }

    //keeps a top ten list
    private static class TopTenList {
        Pair[] list = new Pair[10];

        TopTenList() {
            for (int i = 0; i < 10; i++)
                list[i] = new Pair("", 0);
        }

        void print() {
            System.out.println("=== LIST ===");
            for (int i = 0; i < list.length; i++) {
                Pair pair = list[i];
                System.out.printf("[%d] %d %s\n", i, pair.n, pair.link);
            }
        } 

        //adds a Pair if it's in the top ten
        void add(String link, int n) {
            for (int i = 0; i < 10; i++) {
                if (n > list[i].n) {
                    for (int j = 9; j > i; j--) {
                        list[j] = list[j-1];
                    }
                    list[i] = new Pair(link, n);
                    break;
                }
            }
        }
    }

    //reduces set to just the top ten for each type
    public static class TopTenReducer extends
        Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

            TopTenList list = new TopTenList();

            for (Text value : values) {
                String[] items = value.toString().split("\t");
                String link = items[0];
                int n = Integer.parseInt(items[1]);
                list.add(link, n);
            }
            for (Pair pair : list.list )
                context.write(key, new Text(pair.link + "\t" + pair.n));
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job;

		job = Job.getInstance(conf, "Top Ten Part 1");
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/top_ten_part_1"));
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Top Ten Part 2");
		job.setJarByClass(TopTen.class);
		job.setMapperClass(OutputMapper.class);
		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("output/top_ten_part_1"));
		FileOutputFormat.setOutputPath(job, new Path("output/top_ten_part_2"));
		job.waitForCompletion(true);
	}
}
