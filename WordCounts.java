/**
* Returns a file containing Strings and number of times they occurred in the documents.
* Here the map program takes the input file and splits them whenever the specified delimiter is encountered.
* The map result is in the format <Text, Count(integer)>.
* This Key-Value pair is then fed to the Reducer where it aggregates the number of times a particular string was found in the document.
* 
* @param FileInputFormat is used to specify the input path
* @param FileOutputFormat specifies the output directory where the result will be stored.
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {
	public static class WordCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ArrayList<String> arrays = new ArrayList<>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> worddoc = Arrays.asList(value.toString().split("\\s+"));
			for(int pivot=0; pivot<worddoc.size()-4;pivot++)
			{
				word.set(worddoc.subList(pivot, pivot+5).toString().replace("[", "").replace("]","").replace(",", ""));
				context.write(word, one);
			}
/*			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				arrays.add(itr.nextToken());
			}
			// System.out.println(arrays);
			
			while (itr.hasMoreTokens()) {
				// for (i0nt i=0; i<5; i++) {
				//	word.set(itr.nextToken());
				//	arrays.add(e)
				context.write(word,  one);
			}*/
		}
	}
	public static class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce (Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	public static void main (String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		
		job.setJarByClass(WordCount.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/cloudera/workspace/FiveWordCount/input/Sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/workspace/FiveWordCount/output"));
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0: 1);
	}
}

