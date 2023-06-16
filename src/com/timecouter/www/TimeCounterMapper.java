package com.timecouter.www;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class TimeCounterMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString()); //Dividing String into tokens
		while (itr.hasMoreTokens()){
			word.set(itr.nextToken());
			context.write(word, one); //new IntWritable(1)
		}
	}
}
