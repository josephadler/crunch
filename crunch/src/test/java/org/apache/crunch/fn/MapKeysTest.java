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
package org.apache.crunch.fn;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.Pair;
import org.junit.Test;

@SuppressWarnings("serial")
public class MapKeysTest {

  protected static final MapKeysFn<String, Integer, Integer> one = new MapKeysFn<String, Integer, Integer>() {
    @Override
    public Integer map(String input) {
      return 1;
    }
  };

  protected static final MapKeysFn<String, Integer, Integer> two = new MapKeysFn<String, Integer, Integer>() {
    @Override
    public Integer map(String input) {
      return 2;
    }
  };

  @Test
  public void test() {
    StoreLastEmitter<Pair<Integer, Integer>> emitter = StoreLastEmitter.create();
    one.process(Pair.of("k", Integer.MAX_VALUE), emitter);
    assertEquals(Pair.of(1, Integer.MAX_VALUE), emitter.getLast());
    two.process(Pair.of("k", Integer.MAX_VALUE), emitter);
    assertEquals(Pair.of(2, Integer.MAX_VALUE), emitter.getLast());
  }

}
