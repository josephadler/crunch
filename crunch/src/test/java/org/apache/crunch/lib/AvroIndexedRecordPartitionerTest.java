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
package org.apache.crunch.lib;

import static org.junit.Assert.assertEquals;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.crunch.lib.join.JoinUtils.AvroIndexedRecordPartitioner;
import org.junit.Before;
import org.junit.Test;

public class AvroIndexedRecordPartitionerTest {

  private AvroIndexedRecordPartitioner avroPartitioner;

  @Before
  public void setUp() {
    avroPartitioner = new AvroIndexedRecordPartitioner();
  }

  @Test
  public void testGetPartition() {
    IndexedRecord indexedRecord = new MockIndexedRecord(3);
    AvroKey<IndexedRecord> avroKey = new AvroKey<IndexedRecord>(indexedRecord);

    assertEquals(3, avroPartitioner.getPartition(avroKey, new AvroValue<Object>(), 5));
    assertEquals(1, avroPartitioner.getPartition(avroKey, new AvroValue<Object>(), 2));
  }

  @Test
  public void testGetPartition_NegativeHashValue() {
    IndexedRecord indexedRecord = new MockIndexedRecord(-3);
    AvroKey<IndexedRecord> avroKey = new AvroKey<IndexedRecord>(indexedRecord);

    assertEquals(3, avroPartitioner.getPartition(avroKey, new AvroValue<Object>(), 5));
    assertEquals(1, avroPartitioner.getPartition(avroKey, new AvroValue<Object>(), 2));
  }

  @Test
  public void testGetPartition_IntegerMinValue() {
    IndexedRecord indexedRecord = new MockIndexedRecord(Integer.MIN_VALUE);
    AvroKey<IndexedRecord> avroKey = new AvroKey<IndexedRecord>(indexedRecord);

    assertEquals(0, avroPartitioner.getPartition(avroKey, new AvroValue<Object>(), Integer.MAX_VALUE));
  }

  /**
   * Mock implementation of IndexedRecord to give us control over the hashCode.
   */
  static class MockIndexedRecord implements IndexedRecord {

    private Integer value;

    public MockIndexedRecord(Integer value) {
      this.value = value;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public Schema getSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int arg0) {
      return this.value;
    }

    @Override
    public void put(int arg0, Object arg1) {
      throw new UnsupportedOperationException();
    }

  }

}
