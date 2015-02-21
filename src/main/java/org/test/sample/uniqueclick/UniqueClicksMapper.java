package org.test.sample.uniqueclick;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UniqueClicksMapper extends Mapper<LongWritable, Text, Text, Text> {
	// Map<String, Integer> sitedata = new HashMap<String, Integer>();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] val = line.split(" ");

		if (val.length > 2) {
			if (val[3].contains("2002")) {
				String ip = val[0];
				String url = val[10];
				// sitedata.put(url,ip);

				context.write(new Text(url), new Text(ip));
			}
		}

	}
}
