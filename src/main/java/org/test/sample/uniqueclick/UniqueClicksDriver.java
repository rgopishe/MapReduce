package org.test.sample.uniqueclick;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.test.sample.WordCountDriver;
import org.test.sample.WordCountMapper;
import org.test.sample.WordCountReducer;

public class UniqueClicksDriver {


public static void main(String[] args) throws Exception {
	if (args.length != 2) {
	System.err.println("Site count: <input path> <output path>");
	System.exit(-1);
	}
	
	Job job = new Job();
	job.setJarByClass(UniqueClicksDriver.class);
	job.setJobName("ClickCount");
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(UniqueClicksMapper.class);
	job.setReducerClass(UniqueClicksReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);	
	job.setOutputValueClass(IntWritable.class);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}
