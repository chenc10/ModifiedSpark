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

import scala.collection.mutable.HashMap

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

/** Computes an approximation to pi */
object CCTest {
  def waiting(time: Int) : Unit = {
    // time in microsecond
    val start = System.nanoTime()
    while (System.nanoTime() < start + time * 1000000L){}
  }

  def waitMap(time: Int, arg: (Int, Int)): (Int, Int) = {
     waiting(time)
     arg
    }

   def job0(duration: Int, spark: SparkContext): rdd.RDD[(Int, Int)] = {
     val value01 = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => waitMap(200, i))

     val value02 = value01.reduceByKey((x, y) => x + y, 2)
     val value03 = value02.map( i => (i._1 % 2, i._2)).map(i => waitMap(400, i))

     val value11 = spark.parallelize(0 until 4, 2).map(i => (i % 2, i)).map(i => waitMap(400, i))

     val value04 = value03.reduceByKey((x, y) => x + y, 2)
     val value12 = value11.reduceByKey((x, y) => x + y, 2)
     val value = value12.union(value04).map(i => waitMap(400, i))

     // print("rdd partition size of value: %d" + value.partitions.size.toString)
     value
   }

  def job1(duration: Int, spark: SparkContext): rdd.RDD[(Int, Int)] = {
    val value = spark.parallelize(0 until 16, 4).map(i => (i % 4, i)).map(i => waitMap(200, i))
    value
  }

  def passSchedulingInfo( spark: SparkContext,
                          jobSubmittingSchedule: HashMap[Int, (Int, Int)],
                          taskProfiledInfo: HashMap[(Int, Int), Int]): Unit = {
    var jobSubmittingScheduleString = ""
    val jobSubmittingScheduleBuffer = jobSubmittingSchedule.toBuffer

    for ( i <- 0 until jobSubmittingScheduleBuffer.size) {
      if ( i > 0){
        jobSubmittingScheduleString += " "
      }
      jobSubmittingScheduleString += jobSubmittingScheduleBuffer(i)._1 + "+" +
        jobSubmittingScheduleBuffer(i)._2._1.toString + "+" +
        jobSubmittingScheduleBuffer(i)._2._2.toString
    }

    var taskProfiledInfoString = ""
    val taskInfoBuffer = taskProfiledInfo.toBuffer
    for ( i <- 0 until taskInfoBuffer.size) {
      if ( i > 0){
        taskProfiledInfoString += " "
      }
      taskProfiledInfoString += taskInfoBuffer(i)._1._1.toString + "+" +
        taskInfoBuffer(i)._1._2.toString + "+" + taskInfoBuffer(i)._2.toString
    }
    spark.setLocalProperty("job.profiledInfo", jobSubmittingScheduleString)
    spark.setLocalProperty("stage.profiledInfo", taskProfiledInfoString)
  }

  def submit(submittingTime: Int, currentRDD: rdd.RDD[(Int, Int)]): Unit = {
    waiting(submittingTime)
    currentRDD.collect()
  }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Spark Pi").set("spark.scheduler.mode", "GPS")
        val spark = new SparkContext(conf)
        // Logger.getRootLogger().setLevel(Level.INFO)

        // Format: (jobId, (jobSubmittingTime, jobRunTime); currently not used
        val jobSubmittingSchedule = new HashMap[Int, (Int, Int)]
        // Format: (jobId, taskIdInJob), taskRunTime; currently not used
        val taskProfiledInfo = new HashMap[(Int, Int), Int]
        jobSubmittingSchedule += (0 -> (100, 200), 1 -> (150, 100))
        taskProfiledInfo += ( (0, 0) -> 800, (0, 1) -> 800, (0, 2) -> 800,
          (0, 3) -> 800, (1, 0) -> 800, (1, 1) -> 800, (1, 2) -> 200, (1, 3) -> 500)

        passSchedulingInfo(spark, jobSubmittingSchedule, taskProfiledInfo)

        // format: jobId+jobSubmittingTime+jobRunTime
        // spark.setLocalProperty("job.profiledInfo", "0+100+200 1+150+100")
        // format: jobId+stageId+stageRunTime
        // spark.setLocalProperty("stage.profiledInfo", "0+0+800 0+1+800 0+2+800" +
        //  " 0+3+800 1+0+800 1+1+800 1+2+200 1+3+500")

        val value0 = job0(jobSubmittingSchedule(0)._2, spark)
        val value1 = job1(jobSubmittingSchedule(1)._2, spark)

        Array((0, value0), (1, value1)).par.foreach(i =>
          submit(jobSubmittingSchedule(i._1)._1, i._2))

        spark.stop()
   }
}

