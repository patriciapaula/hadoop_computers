package br.timecounter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounterMapper extends MapReduceBase implements Mapper <LongWritable, Text, LongWritable, Text> {
	private LongWritable k = new LongWritable();
	private Text v = new Text();
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		String[] tokens = value.toString().split("\t");
		if (tokens[0].charAt(0) != '#') {
			Long machine = new Long(tokens[1]); //node name
			if (tokens[2].equals("1")){ //on
				long startTime = parseTimestamp(tokens[3]);
                long endTime = parseTimestamp(tokens[4]);
                
				k.set(machine);
				v.set(String.valueOf(endTime - startTime));
				output.collect(k, v);
			}
		}
	}
	
	private long parseTimestamp(String timestamp) {
        try {
            Date date = dateFormat.parse(timestamp);
            return date.getTime();
        } catch (Exception e) {
            return 0;
        }
    }
	
}
