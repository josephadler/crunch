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

package com.cloudera.crunch.type.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link RecordReader} for Avro data files. */
public class AvroRecordReader<T> extends RecordReader<AvroWrapper<T>, NullWritable> {

	private FileReader<T>[] readers;
	private long[] starts;
	private long[] ends;
	private long[] lengths;
	private long total;
	private long bytesread = 0;
	private int currentReaderIndex = 0;
	private AvroWrapper<T> key;
	private NullWritable value;
	private Schema schema;

	public AvroRecordReader(Schema schema) {
		this.schema = schema;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		DatumReader<T> datumReader = null;
		if (context.getConfiguration().getBoolean(AvroJob.INPUT_IS_REFLECT, true)) {
		  ReflectDataFactory factory = Avros.getReflectDataFactory(conf);
			datumReader = factory.getReader(schema);
		} else {
			datumReader = new SpecificDatumReader<T>(schema);
		}
		if (genericSplit.getClass() == CombineFileSplit.class) {
			CombineFileSplit split = (CombineFileSplit) genericSplit;
			this.readers = new FileReader[split.getNumPaths()];
			this.starts = new long[split.getNumPaths()];
			this.ends = new long[split.getNumPaths()];
			this.lengths = new long[split.getNumPaths()];
			for (int i=0; i < split.getNumPaths(); i++) {
				SeekableInput in = new FsInput(split.getPath(i), conf);
				this.readers[i] = DataFileReader.openReader(in, datumReader);
				this.readers[i].sync(split.getOffset(i));
				this.starts[i] = this.readers[i].tell();
				this.lengths[i] = split.getLength(i);
				this.ends[i] = this.starts[i] + this.lengths[i];
			}
			this.total = split.getLength();
		} else {
			FileSplit split = (FileSplit) genericSplit;
			SeekableInput in = new FsInput(split.getPath(), conf);
			this.readers = new FileReader[1];
			this.readers[0] = DataFileReader.openReader(in, datumReader);
			readers[0].sync(split.getStart()); // sync to start
			this.starts = new long[1];
			this.starts[0] = readers[0].tell();
			this.ends = new long[1];
			this.ends[0] = split.getStart() + split.getLength();
			this.lengths = new long[1];
			this.lengths[0] = split.getLength();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!readers[currentReaderIndex].hasNext() 
				|| readers[currentReaderIndex].pastSync(ends[currentReaderIndex])) {
			key = null;
			value = null;
			return false;
		}
		if (key == null) {
			key = new AvroWrapper<T>();
		}
		if (value == null) {
			value = NullWritable.get();
		}
		key.datum(readers[currentReaderIndex].next(key.datum()));
		if ( (!readers[currentReaderIndex].hasNext() 
				|| readers[currentReaderIndex].pastSync(ends[currentReaderIndex])
				) 
				&& currentReaderIndex < (readers.length - 1)) {
			bytesread += lengths[currentReaderIndex];
			currentReaderIndex++;
		}
		return true;
	}

	@Override
	public AvroWrapper<T> getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		return Math.min(1.0f, (getPos() + bytesread) / (float)(total));
	}

	public long getPos() throws IOException {
		return (readers[currentReaderIndex].tell() - starts[currentReaderIndex]) + bytesread;
	}

	@Override
	public void close() throws IOException {
		for (FileReader<T> r : readers) {
			r.close();
		}
	}
}