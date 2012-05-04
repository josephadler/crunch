package com.cloudera.crunch.impl.mr.run;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CrunchCombineInputFormat<K, V> extends CrunchInputFormat<K, V> {
	  @Override
	  public RecordReader<K, V> createRecordReader(InputSplit inputSplit,
	      TaskAttemptContext context) throws IOException, InterruptedException {
	    return (RecordReader<K, V>) new CrunchCombineRecordReader<K,V>(inputSplit, context);
	  }	  
}
