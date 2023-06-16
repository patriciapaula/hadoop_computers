package com.timecouter.www;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class TimeCounterReducer extends MapReduceBase implements Reducer <LongWritable, Text, LongWritable, Text> {
	private Text v = new Text();
	
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		Long sum = new Long(0);
		Long traceStart = new Long(Long.MAX_VALUE);
		Long traceEnd = new Long(0);
		Long start = new Long(0);
		Long end = new Long(0);
				
		while (values.hasNext()) {
			String line = values.next().toString();
			String[] tokens = line.split(":");
			start = new Double(tokens[0]).longValue();
			end = new Double(tokens[1]).longValue();
			
			if (start < traceStart) {
				traceStart = start;
			}
			if (end > traceEnd) {
				traceEnd = end;
			}
			sum += (end-start);
		}
		
		v = new Text (Long.toString(sum));
		output.collect(key, v);
	}
}
