package org.test.sample;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	// Reduce method for just outputting the key from mapper as the value from
	// mapper is just an empty string
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();

		}
		context.write(key, new IntWritable(sum));
	}
}