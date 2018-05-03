package mapReduce;

/**
 * @author Sagar Jain
 * Problem Statement - To get a max length word from the input file.
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxWordLenght {

	//Mapper
	public static class Map extends Mapper<IntWritable, Text, Text, IntWritable>{
		public void map (IntWritable key, Text value, Context context) throws
		IOException, InterruptedException {
			String line = value.toString();
			String [] wordArray = line.split(" ");
			
			for(String word : wordArray) {
				if(word.length() >0) {
					value.set(word);
					context.write(value, new IntWritable(word.length()));
				}
			}
		}
	}
	
	//Reducer
	// First setup method will get called once for initialization
	// Then map or reduce method will get called many times
	// at last cleanup method get called
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		String maxWord;
		
		public void setup(Context context) {
			maxWord = new String();
		}
		
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			if(key.toString().length() > maxWord.length())
				maxWord = key.toString();
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(maxWord), new IntWritable(maxWord.length()));
		}
	}
	
	//Driver
	public static void main(String s[]) throws Exception {
		org.apache.hadoop.conf.Configuration conf= new org.apache.hadoop.conf.Configuration();
		Job job = new Job(conf, "SagarMaxWordLenght");
		job.setJarByClass(MaxWordLenght.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(s[0]));
		TextOutputFormat.setOutputPath(job, new Path(s[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
