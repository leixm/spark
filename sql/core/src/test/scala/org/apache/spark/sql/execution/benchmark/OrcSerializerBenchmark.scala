/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.execution.datasources.orc.OrcSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure [[OrcSerializer.serialize]] performance when the schema
 * contains complex map types.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *        bin/spark-submit --class <this class>
 *          --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *        "benchmarks/OrcSerializerBenchmark-results.txt".
 * }}}
 */
object OrcSerializerBenchmark extends SqlBasedBenchmark {

  private val random = new Random(42)

  // Schema: id BIGINT, attrs MAP<STRING, MAP<STRING, INT>>
  private val schema: StructType = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("attrs",
      MapType(StringType, MapType(StringType, IntegerType, valueContainsNull = false),
        valueContainsNull = true),
      nullable = true)
  ))

  private val serializer = new OrcSerializer(schema)

  private def createNestedMap(i: Long): MapData = {
    val inner1 = Map(
      "a" -> (i.toInt & 0x7fffffff),
      "b" -> ((i.toInt + 1) & 0x7fffffff)
    )
    val inner2 = Map(
      "c" -> ((i.toInt + 2) & 0x7fffffff),
      "d" -> ((i.toInt + 3) & 0x7fffffff)
    )

    val inner1Map = ArrayBasedMapData(
      inner1,
      keyConverter = k => UTF8String.fromString(k.toString),
      valueConverter = v => v
    )
    val inner2Map = ArrayBasedMapData(
      inner2,
      keyConverter = k => UTF8String.fromString(k.toString),
      valueConverter = v => v
    )

    val outer = Map[UTF8String, MapData](
      UTF8String.fromString(s"group1-$i") -> inner1Map,
      UTF8String.fromString(s"group2-$i") -> inner2Map
    )

    ArrayBasedMapData(
      outer,
      keyConverter = k => k,
      valueConverter = v => v
    )
  }

  private def generateRows(numRows: Int): Array[InternalRow] = {
    val rows = new Array[InternalRow](numRows)
    var idx = 0
    while (idx < numRows) {
      val id = idx.toLong
      val row = new GenericInternalRow(2)
      row.update(0, id)
      row.update(1, createNestedMap(id))
      rows(idx) = row
      idx += 1
    }
    rows
  }

  private def runSerializeBenchmark(numRows: Int): Unit = {
    val rows = generateRows(numRows)
    val benchmarkName = s"OrcSerializer.serialize with nested map ($numRows rows)"
    val benchmark = new Benchmark(benchmarkName, numRows, output = output)

    benchmark.addCase("serialize") { _ =>
      var i = 0
      while (i < rows.length) {
        serializer.serialize(rows(i))
        i += 1
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("OrcSerializer nested map serialization") {
      // 10 / 1w / 10w / 100w
      Seq(10, 10000, 100000, 1000000).foreach(runSerializeBenchmark)
    }
  }
}


