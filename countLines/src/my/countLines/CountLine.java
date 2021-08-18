package my.countLines;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountLine {
	// MAPPER CODE
	public static class CountLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	    Text placeholder = new Text("Total Lines");
	    private final static IntWritable one = new IntWritable(1);
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	    	context.write(placeholder, one);
	    }
	}

	// REDUCER CODE
	public static class LineCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
	        for (IntWritable value : values) {
	            sum += value.get();
	        }
	        context.write(key, new IntWritable(sum));
		}
	}

	// DRIVER CODE
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJobName("Count Lines");
        job.setJarByClass(CountLine.class);
        job.setMapperClass(CountLineMapper.class);
        job.setReducerClass(LineCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
