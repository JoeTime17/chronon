/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing._
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable

class Timer {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var ts: Long = System.currentTimeMillis()

  def start(): Unit = { ts = System.currentTimeMillis() }

  // TODO: Write this out into a file checked into git
  // or incorporate proper benchmarks
  def publish(name: String, reset: Boolean = true): Unit = {
    logger.info(s"${name.padTo(25, ' ')} ${System.currentTimeMillis() - ts} ms")
    if (reset) ts = System.currentTimeMillis()
  }
}

class SawtoothAggregatorTest extends AnyFlatSpec {

  it should "tail accuracy" in {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 10000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))))
    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val hopsAggregator = new HopsAggregator(
      queries.min,
      aggregations,
      schema,
      FiveMinuteResolution
    )
    var hops1 = hopsAggregator.init()
    var hops2 = hopsAggregator.init()
    var hopsAll = hopsAggregator.init()

    for (i <- 0 until events.length / 2) {
      hops1 = hopsAggregator.update(hops1, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }

    for (i <- events.length / 2 until events.length) {
      hops2 = hopsAggregator.update(hops2, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }
    timer.publish("hops/Update")

    val hopsMerged = hopsAggregator.merge(hops1, hops2)
    val mergedHops = hopsAggregator.toTimeSortedArray(hopsMerged)
    val rawHops = hopsAggregator.toTimeSortedArray(hopsAll)
    timer.publish("hops/Sort")

    val gson = new Gson
    val mergedStr = gson.toJson(mergedHops)
    val rawStr = gson.toJson(rawHops)
    assertEquals(mergedStr, rawStr)

    timer.start()
    val sawtoothIrs = sawtoothAggregator.computeWindows(rawHops, queries)
    timer.publish("sawtooth/ComputeWindows")

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      sawtoothAggregator.windowedAggregator,
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naive/Aggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    for (i <- queries.indices) {
      val naiveStr = gson.toJson(naiveIrs(i))
      val sawtoothStr = gson.toJson(sawtoothIrs(i))
      assertEquals(naiveStr, sawtoothStr)
    }
  }

  it should "real time accuracy" in {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val columns = Seq(Column("ts", LongType, 180),
                      Column("num", LongType, 1000),
                      Column("age", LongType, 100),
                      Column("bucket1", StringType, 3),
                      Column("bucket2", StringType, 2))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE,
                           "num",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           ),
                           buckets = Seq("bucket1", "bucket2")),
      Builders.Aggregation(Operation.AVERAGE, "age", buckets = Seq("bucket1")),
      Builders.Aggregation(Operation.AVERAGE,
                           "age",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           )),
      Builders.Aggregation(Operation.SUM, "age")
    )
    timer.publish("setup")
    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    timer.publish("sawtoothAggregate")

    val unpacked = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = unpacked.map(FiveMinuteResolution.calculateTailHop)
    val rowAgg = new RowAggregator(schema, aggregations.flatMap(_.unpack))
    val naiveAggregator = new NaiveAggregator(
      rowAgg,
      unpacked,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naiveAggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    val gson = new Gson
    for (i <- queries.indices) {
      val naiveStr = gson.toJson(rowAgg.finalize(naiveIrs(i)))
      val sawtoothStr = gson.toJson(rowAgg.finalize(sawtoothIrs(i)))
      assertEquals(naiveStr, sawtoothStr)
    }
    timer.publish("comparison")
  }

  it should "include events with same timestamp as query (temporal window boundary)" in {
    // Test for the cumulate method fix - events at exact query timestamp should be included
    val baseTimestamp = 1700000000000L // Fixed base timestamp

    // Create events at specific timestamps
    val columns = Seq(Column("ts", LongType, 180), Column("value", LongType, 100))
    val schema = columns.map(_.schema)

    // Events: [500ms, 2000ms, 2000ms (duplicate), 2500ms, 4000ms]
    val events = Array(
      new TestRow(baseTimestamp + 500, 10L)(0),     // Event A: before first query
      new TestRow(baseTimestamp + 2000, 100L)(0),   // Event B: exact match with query
      new TestRow(baseTimestamp + 2000, 200L)(0),   // Event C: duplicate timestamp
      new TestRow(baseTimestamp + 2500, 50L)(0),    // Event D: between queries
      new TestRow(baseTimestamp + 4000, 300L)(0)    // Event E: after all queries
    )

    // Queries: [1000ms, 2000ms, 2000ms (duplicate), 3000ms]
    val queries = Array(
      baseTimestamp + 1000,  // Query 0: should include Event A only
      baseTimestamp + 2000,  // Query 1: should include Event A, B, C (events at exact timestamp)
      baseTimestamp + 2000,  // Query 2: duplicate - should also include A, B, C
      baseTimestamp + 3000   // Query 3: should include Event A, B, C, D
    )

    // Aggregation: COUNT and SUM with 1-day window
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "value", Seq(new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(1, TimeUnit.DAYS)))
    )

    val result = sawtoothAggregate(events, queries, aggregations, schema)
    val rowAgg = new RowAggregator(schema, aggregations.flatMap(_.unpack))

    // Verify results
    assertEquals(queries.length, result.length)

    // Query 0 @ 1000ms: includes Event A (500ms)
    val result0 = rowAgg.finalize(result(0))
    assertEquals(1L, result0(0))  // COUNT = 1
    assertEquals(10L, result0(1)) // SUM = 10

    // Query 1 @ 2000ms: includes Event A (500ms) + Event B (2000ms) + Event C (2000ms)
    // This is the critical test - events at exact timestamp should be included
    val result1 = rowAgg.finalize(result(1))
    assertEquals(3L, result1(0))  // COUNT = 3 (not 1!)
    assertEquals(310L, result1(1)) // SUM = 10 + 100 + 200 = 310

    // Query 2 @ 2000ms: same as Query 1 (duplicate timestamp)
    val result2 = rowAgg.finalize(result(2))
    assertEquals(3L, result2(0))  // COUNT = 3
    assertEquals(310L, result2(1)) // SUM = 310

    // Query 3 @ 3000ms: includes Event A + B + C + D
    val result3 = rowAgg.finalize(result(3))
    assertEquals(4L, result3(0))  // COUNT = 4
    assertEquals(360L, result3(1)) // SUM = 10 + 100 + 200 + 50 = 360

    // Event E at 4000ms should not be included in any query (it's in the future)
  }

}

object SawtoothAggregatorTest {
  // the result is irs in sorted order of queries
  // with head real-time accuracy and tail hop accuracy
  // NOTE: This provides a sketch for a distributed topology
  def sawtoothAggregate(
      events: Array[TestRow],
      queries: Array[Long],
      specs: Seq[Aggregation],
      schema: Seq[(String, DataType)],
      resolution: Resolution = FiveMinuteResolution
  ): Array[Array[Any]] = {

    // STEP-1. build hops
    val hopsAggregator =
      new HopsAggregator(queries.min, specs, schema, resolution)
    val sawtoothAggregator =
      new SawtoothAggregator(specs, schema, resolution)
    var hopMaps = hopsAggregator.init()
    for (i <- events.indices)
      hopMaps = hopsAggregator.update(hopMaps, events(i))
    val hops = hopsAggregator.toTimeSortedArray(hopMaps)

    val minResolution = resolution.hopSizes.min
    // STEP-2. group events and queries by headStart - 5minute round down of ts
    val groupedQueries: Map[Long, Array[Long]] =
      queries.groupBy(TsUtils.round(_, minResolution))
    val groupedEvents: Map[Long, Array[TestRow]] = events.groupBy { row =>
      TsUtils.round(row.ts, minResolution)
    }

    // STEP-3. compute windows based on headStart
    // aggregates will have at-most 5 minute staleness
    val headStartTimes = groupedQueries.keys.toArray
    util.Arrays.sort(headStartTimes)
    // compute windows up-to 5min accuracy for the queries
    val nonRealtimeIrs = sawtoothAggregator.computeWindows(hops, headStartTimes)

    val result = mutable.ArrayBuffer.empty[Array[Any]]
    // STEP-4. join tailAccurate - Irs with headTimeStamps and headEvents
    // to achieve realtime accuracy
    for (i <- headStartTimes.indices) {
      val headStart = headStartTimes(i)
      val tailIr = nonRealtimeIrs(i)

      // join events and queries on tailEndTimes
      val endTimes: Array[Long] = groupedQueries.getOrElse(headStart, null)
      val headEvents = groupedEvents.getOrElse(headStart, null)
      if (endTimes != null && headEvents != null) {
        util.Arrays.sort(endTimes)
      }

      result ++= sawtoothAggregator.cumulate(
        Option(headEvents).map(_.iterator).orNull,
        endTimes,
        tailIr
      )
    }
    result.toArray
  }
}
