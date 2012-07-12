/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.types.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for Avro data files. */
public class AvroInputFormat<T> extends CombineFileInputFormat<AvroWrapper<T>, NullWritable> {

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
	  List<FileStatus> result = new ArrayList<FileStatus>();
      for (FileStatus file : super.listStatus(job)) {
        if (file.getPath().getName().endsWith(org.apache.avro.mapred.AvroOutputFormat.EXT)) {
          result.add(file);
		}
      }
      return result;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		if (getMaxSplitSize(job) == 0) {
			this.setMaxSplitSize(2 << 28);
		}
		return super.getSplits(job);
	}
	
	@Override
	public RecordReader<AvroWrapper<T>, NullWritable> createRecordReader(InputSplit split,
		TaskAttemptContext context) throws IOException {
      context.setStatus(split.toString());
      String jsonSchema = context.getConfiguration().get(AvroJob.INPUT_SCHEMA);
      Schema schema = new Schema.Parser().parse(jsonSchema);
      return new AvroRecordReader<T>(schema);
	}

}
