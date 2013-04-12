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
package org.apache.crunch.impl;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class SingleUseIterableTest {

  @Test
  public void testIterator() {
    List<Integer> values = Lists.newArrayList(1,2,3);
    
    SingleUseIterable<Integer> iterable = new SingleUseIterable<Integer>(values);

    List<Integer> retrievedValues = Lists.newArrayList(iterable);
    
    assertEquals(values, retrievedValues);
  }
  
  @Test(expected=IllegalStateException.class)
  public void testIterator_MultipleCalls() {
    List<Integer> values = Lists.newArrayList(1,2,3);
    
    SingleUseIterable<Integer> iterable = new SingleUseIterable<Integer>(values);

    List<Integer> retrievedValues = Lists.newArrayList(iterable);

    for (Integer n : iterable) {
      
    }
  }

}
