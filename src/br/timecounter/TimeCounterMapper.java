package br.timecounter;


import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounterMapper extends MapReduceBase implements Mapper <LongWritable, Text, LongWritable, Text> {
	private LongWritable k = new LongWritable();
	private Text v = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		String[] tokens = value.toString().split("\\s");
		if (tokens[0].charAt(0) != '#') {
			Long machine = new Long(tokens[1]);
			if (tokens[2].equals("1")){
				k.set(machine);
				v.set(tokens[3]+":"+tokens[4]);
				output.collect(k, v);
			}
		}
	}
}
