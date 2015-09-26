/** Copyright(c) 2012 Orzota, Inc. All Rights Reserved. **/
package com.orzota.bookx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @organisaion Orzota, Inc.
 * @author varadmeru
 */
 /*
 Updating for new MR API
 */
public class BookXMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable one = new IntWritable(1);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
	 * org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(LongWritable _key, Text value,
			Context context)
			throws IOException, InterruptedException
	{

		String TempString = value.toString();
		String[] SingleBookData = TempString.split("\";\"");
		context.write(new Text(SingleBookData[3]), one);
	}
}
