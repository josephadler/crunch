package com.cloudera.crunch.impl.mr.run;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CrunchCombineRecordReader<K, V> extends CombineFileRecordReader<K, V> {

	public CrunchCombineRecordReader(InputSplit inputSplit,
			TaskAttemptContext context)
			throws IOException {
		super((CombineFileSplit) inputSplit, context, 
				(Class<? extends RecordReader<K, V>>) com.cloudera.crunch.impl.mr.run.CrunchRecordReader.class);
	}

}
