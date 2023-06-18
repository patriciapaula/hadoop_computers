package br.timecounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounterReducer extends MapReduceBase implements Reducer <LongWritable, Text, LongWritable, Text> {
	private Text v = new Text();
	
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		Long sum = new Long(0);
		for (Long value : convertIteratorToList(values)) {
            sum += value;
        }
		v = new Text (Long.toString(sum));
		output.collect(key, v);
	}
	
	private ArrayList<Long> convertIteratorToList(Iterator<Text> values) {
		List<Long> list = new ArrayList<>();
		while (values.hasNext()) {
            Long value = Long.parseLong(String.valueOf(values.next()));
            list.add(value);
        }
		return (ArrayList<Long>) list;
    }
	
}
