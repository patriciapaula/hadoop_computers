package com.timecouter.www;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;//GenericOptionsParser;

public class TimeCounter {
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (pathArgs.length < 2)
		{
			System.err.println("Usage: wordcount <input-path> […] <output-path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MapReduce TimeCount");
		job.setJarByClass(TimeCounter.class);
		job.setMapperClass(TimeCounterMapper.class);
		job.setCombinerClass(TimeCounterReducer.class);
		job.setReducerClass(TimeCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < pathArgs.length - 1; ++i){
			FileInputFormat.addInputPath(job, new Path(pathArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(pathArgs[pathArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
