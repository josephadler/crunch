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
package org.apache.crunch.impl.mr.exec;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchJobControl;

import com.google.common.collect.Lists;

/**
 *
 *
 */
public class MRExecutor {

  private static final Log LOG = LogFactory.getLog(MRExecutor.class);

  private final CrunchJobControl control;

  public MRExecutor(Class<?> jarClass) {
    this.control = new CrunchJobControl(jarClass.toString());
  }

  public void addJob(CrunchJob job) {
    this.control.addJob(job);
  }

  public PipelineResult execute() {
    try {
      Thread controlThread = new Thread(control);
      controlThread.start();
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      control.stop();
    } catch (InterruptedException e) {
      LOG.info(e);
    }
    List<CrunchControlledJob> failures = control.getFailedJobList();
    if (!failures.isEmpty()) {
      System.err.println(failures.size() + " job failure(s) occurred:");
      for (CrunchControlledJob job : failures) {
        System.err.println(job.getJobName() + "(" + job.getJobID() + "): " + job.getMessage());
      }
    }
    List<PipelineResult.StageResult> stages = Lists.newArrayList();
    for (CrunchControlledJob job : control.getSuccessfulJobList()) {
      try {
        stages.add(new PipelineResult.StageResult(job.getJobName(), job.getJob().getCounters()));
      } catch (Exception e) {
        LOG.error("Exception thrown fetching job counters for stage: " + job.getJobName(), e);
      }
    }
    return new PipelineResult(stages);
  }
}
