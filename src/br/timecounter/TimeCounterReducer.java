package br.timecounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TimeCounterReducer extends MapReduceBase implements Reducer <LongWritable, Text, LongWritable, Text> {
	
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		Map<Double, Double> machineTimeMap = new HashMap<>();
		double totalActiveDays = 0.0;
		double averageActiveDays = 0.0;
		
		for (Text value : convertIteratorToList(values)) {
            String[] fields = value.toString().split(",");
            double startTime = Double.parseDouble(fields[0].split(":")[1].trim());
            double endTime = Double.parseDouble(fields[1].split(":")[1].trim());
            double activeDays = (endTime - startTime) / (24 * 60 * 60); // converting to days

            machineTimeMap.put(startTime, endTime);
            totalActiveDays += activeDays;
        }
		averageActiveDays = totalActiveDays / machineTimeMap.size();

		if (averageActiveDays < 300) {
            StringBuilder outputValue = new StringBuilder();
            outputValue.append(String.format("Average active days: %.2f\n", averageActiveDays));

            for (Map.Entry<Double, Double> entry : machineTimeMap.entrySet()) {
                double startTime = entry.getKey();
                double endTime = entry.getValue();
                outputValue.append(String.format("Start time: %.2f, End time: %.2f\n", startTime, endTime));
            }

            output.collect(key, new Text(outputValue.toString()));
        } 
	}
	
	private ArrayList<Text> convertIteratorToList(Iterator<Text> values) {
		List<Text> list = new ArrayList<>();
		while (values.hasNext()) {
            list.add(values.next());
        }
		return (ArrayList<Text>) list;
    }
	
}
