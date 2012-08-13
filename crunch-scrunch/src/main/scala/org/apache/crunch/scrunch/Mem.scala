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
package org.apache.crunch.scrunch

import java.lang.{Iterable => JIterable}

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration

import org.apache.crunch.{Pair => P}
import org.apache.crunch.{Source, TableSource, Target}
import org.apache.crunch.impl.mem.MemPipeline
import org.apache.crunch.scrunch.Conversions._

/**
 * Object for working with in-memory PCollection and PTable instances.
 */
object Mem extends MemEmbeddedPipeline with PipelineHelper {
  private val ptf = Avros

  /**
   * Constructs a PCollection using in memory data.
   *
   * @param collect The data to load.
   * @return A PCollection containing the specified data.
   */
  def collectionOf[T](ts: T*)(implicit pt: PTypeH[T]): PCollection[T] = {
    collectionOf(List(ts:_*))
  }

  /**
   * Constructs a PCollection using in memory data.
   *
   * @param collect The data to load.
   * @return A PCollection containing the specified data.
   */
  def collectionOf[T](collect: Iterable[T])(implicit pt: PTypeH[T]): PCollection[T] = {
    val native = MemPipeline.typedCollectionOf(pt.get(ptf), asJavaIterable(collect))
    new PCollection[T](native)
  }

  /**
   * Constructs a PTable using in memory data.
   *
   * @param pairs The data to load.
   * @return A PTable containing the specified data.
   */
  def tableOf[K, V](pairs: (K, V)*)(implicit pk: PTypeH[K], pv: PTypeH[V]): PTable[K, V] = {
    tableOf(List(pairs:_*))
  }

  /**
   * Constructs a PTable using in memory data.
   *
   * @param pairs The data to load.
   * @return A PTable containing the specified data.
   */
  def tableOf[K, V](pairs: Iterable[(K, V)])(implicit pk: PTypeH[K], pv: PTypeH[V]): PTable[K, V] = {
    val cpairs = pairs.map(kv => P.of(kv._1, kv._2))
    val ptype = ptf.tableOf(pk.get(ptf), pv.get(ptf))
    new PTable[K, V](MemPipeline.typedTableOf(ptype, asJavaIterable(cpairs)))
  }

  /** Contains factory methods used to create `Source`s. */
  val from = From

  /** Contains factory methods used to create `Target`s. */
  val to = To

  /** Contains factory methods used to create `SourceTarget`s. */
  val at = At
}
