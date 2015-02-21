package org.test.sample;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// taking one line at a time and tokenizing the same
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
		String	word=tokenizer.nextToken();

			context.write(new Text(word), new IntWritable(1));
		}
	}

}