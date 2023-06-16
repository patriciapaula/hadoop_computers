package br.timecounter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounter {
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(TimeCounter.class);
		conf.setJobName("tempocount");
		conf.setNumReduceTasks(Integer.parseInt(args[2]));
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(TimeCounterMapper.class);
		conf.setReducerClass(TimeCounterReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
