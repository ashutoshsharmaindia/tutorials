/** Copyright(c) 2012 Orzota, Inc. All Rights Reserved. **/
package com.orzota.bookx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @organisaion Orzota, Inc.
 * @author varadmeru
 */
 
 /*
 Updating for new MR API
 */
public class BookXReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
	 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
	 * org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void reduce(Text _key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException
	{
		Text key = _key;
		int frequencyForYear = 0;
		for(IntWritable value : values)
		{
			// replace ValueType with the real type of your value
			frequencyForYear += value.get();
			// process value
		}
		context.write(key, new IntWritable(frequencyForYear));
	}
}
