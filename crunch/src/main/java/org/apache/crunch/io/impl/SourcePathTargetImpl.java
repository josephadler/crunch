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
package org.apache.crunch.io.impl;

import org.apache.crunch.Source;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class SourcePathTargetImpl<T> extends SourceTargetImpl<T> implements PathTarget {

  public SourcePathTargetImpl(Source<T> source, PathTarget target) {
    super(source, target);
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    ((PathTarget) target).configureForMapReduce(job, ptype, outputPath, name);
  }

  @Override
  public Path getPath() {
    return ((PathTarget) target).getPath();
  }
}
