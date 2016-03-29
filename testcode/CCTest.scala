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

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark._

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math.random

/** Computes an approximation to pi */
object CCTest {

    def func1(time: Int, arg: (Int, Int)): (Int, Int) = {
     val start = System.nanoTime()
     while (System.nanoTime() < start + time * 100000000L){}
     arg
    }

    def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Spark Pi").set("spark.scheduler.mode", "GPS")
     val spark = new SparkContext(conf)
     spark.setLocalProperty("job.jobSubmittingTime", "100")
     spark.setLocalProperty("job.jobRunTime", "50")
     spark.setLocalProperty("stage.stageRunTime", "0 0 800+0 1 800+0 2 800+0 3 800")
     Logger.getRootLogger().setLevel(Level.INFO)

     val value01 = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))

     val value02 = value01.reduceByKey((x, y) => x + y, 2)
     val value03 = value02.map( i => (i._1 % 2, i._2)).map(i => func1(4, i))

     val value11 = spark.parallelize(0 until 4, 2).map(i => (i % 2, i)).map(i => func1(4, i))

     val value04 = value03.reduceByKey((x, y) => x + y, 2)
     val value12 = value11.reduceByKey((x, y) => x + y, 2)
     val value = value12.union(value04).map(i => func1(4, i))

     // print("rdd partition size of value: %d" + value.partitions.size.toString)

     value.collect()
     spark.stop()
   }
}

