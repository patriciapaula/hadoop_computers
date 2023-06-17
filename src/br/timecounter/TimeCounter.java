package br.timecounter;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounter {
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(TimeCounter.class);
		conf.setJobName(args[0]); //"TimeCounter"
		conf.setNumReduceTasks(Integer.parseInt(args[1]));
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(TimeCounterMapper.class);
		conf.setReducerClass(TimeCounterReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf, new Path(args[3]));
		
		JobClient.runJob(conf);
	}
}
