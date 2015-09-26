/** Copyright(c) 2012 Orzota, Inc. All Rights Reserved. **/
package com.orzota.bookx;

// imported jars
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @organisaion Orzota, Inc.
 * @author varadmeru
 */
 /*
 Update history : Updating for newer Hadoop API
 */
public class BookXDriver
{
	public static void main(String[] args) throws Exception
	{
		@SuppressWarnings("deprecation")
		Job job = new Job();
		

		// Name of the Job
		job.setJobName("BookCrossing1.0");
		
		job.setJarByClass(BookXDriver.class);

		// Data type of Output Key and Value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Setting the Mapper and Reducer Class
		job.setMapperClass(com.orzota.bookx.BookXMapper.class);
		job.setReducerClass(com.orzota.bookx.BookXReducer.class);

		// Formats of the Data Type of Input and output			
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
			

		// Specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath( job, new Path(args[0]));
		FileOutputFormat.setOutputPath( job, new Path(args[1]));
		//job.submit();
		//job.waitForCompletion(true);
		
		
			// Running the job with Configurations set in the conf.
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
