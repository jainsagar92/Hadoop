package mapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordCount {

	//Mapper
	public static class Map extends Mapper<IntWritable, Text, Text, IntWritable>{
		public void map (IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while(tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				context.write(value, new IntWritable(1));
			}
		}
		
	}
	
	//Reducer
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for(IntWritable i : value) {
				sum+=i.get(); 
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	//Driver
	public static void main(String s[]) throws Exception {
		org.apache.hadoop.conf.Configuration conf= new org.apache.hadoop.conf.Configuration();
		Job job = new Job(conf, "SagarWordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(s[0]));
		TextOutputFormat.setOutputPath(job, new Path(s[1]));
	}
}
