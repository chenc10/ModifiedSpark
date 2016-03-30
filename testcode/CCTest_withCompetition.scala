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

import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

/** Computes an approximation to pi */
object CCTest {

  def func1(time: Int, arg: (Int, Int)): (Int, Int) = {
     val start = System.nanoTime()
     while (System.nanoTime() < start + time * 100000000L){}
     arg
    }

   def job1(duration: Int, spark: SparkContext): rdd.RDD[(Int, Int)] = {
     println("entercc job1")
     val value01 = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))
     println("enteri job1")

     val value02 = value01.reduceByKey((x, y) => x + y, 2)
     val value03 = value02.map( i => (i._1 % 2, i._2)).map(i => func1(4, i))

     val value11 = spark.parallelize(0 until 4, 2).map(i => (i % 2, i)).map(i => func1(4, i))

     val value04 = value03.reduceByKey((x, y) => x + y, 2)
     val value12 = value11.reduceByKey((x, y) => x + y, 2)
     val value = value12.union(value04).map(i => func1(4, i))

     // print("rdd partition size of value: %d" + value.partitions.size.toString)
     println("enter job1")
     value
   }

  def job2(duration: Int, spark: SparkContext): Future[Array[(Int, Int)]] = future{
    val value = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))
    value.collect()
  }

    def main(args: Array[String]) {
        println("aa")
        val conf = new SparkConf().setAppName("Spark Pi").set("spark.scheduler.mode", "GPS")
        // println("bb")
        val spark = new SparkContext(conf)
        // Logger.getRootLogger().setLevel(Level.INFO)

        // format: jobId+jobSubmittingTime+jobRunTime
        spark.setLocalProperty("job.profiledInfo", "0+100+200 1+150+100")
        // format: jobId+stageId+stageRunTime
        spark.setLocalProperty("stage.profiledInfo", "0+0+800 0+1+800 0+2+800" +
          " 0+3+800 1+0+800 1+1+800 1+2+200 1+3+500")

   // val value = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))
      val value = job1(100, spark)
      val value2 = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))

      Array(value, value2).par.foreach(i => i.collect())

        println("hi0")
        // val value = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => func1(2, i))
        // value.collect()
        println("hi")
        spark.stop()
   }
}

