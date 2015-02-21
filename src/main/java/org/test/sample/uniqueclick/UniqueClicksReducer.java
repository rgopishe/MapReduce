package org.test.sample.uniqueclick;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueClicksReducer extends Reducer<Text, Text, Text, IntWritable> {
	

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// sitedata.put(key,values);
		Set dataset = new HashSet();

		for (Text value : values) {
			// count += value.get();
			dataset.add(value);

		}
		int count = dataset.size();
		context.write(key, new IntWritable(count));
	}
}