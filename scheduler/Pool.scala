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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

// add by cc
import scala.io.Source

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */

private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable
  with Logging {

  // add by cc
  class Event(){
   // logInfo("Event Time: %d".format(time))
    var realTime = System.currentTimeMillis()
    var virtualTime = 0
    var nextT = Long.MaxValue
    var fairShareRate = 0.toDouble
    var activeJobNameList = new ArrayBuffer[(String, Int)]

    def addJob(cname: String): Unit = {
      val currentTime = System.currentTimeMillis()
    logInfo("$$$$$\n $$$$$$$$$$ enter currentTime (%d) Vs nextT (%d)".format(currentTime%1000000,
      nextT%1000000))
      while (currentTime > nextT){
        virtualTime = virtualTime + ((nextT - realTime)*fairShareRate).toInt
        realTime = nextT
        activeJobNameList.remove(0)
        if(activeJobNameList.size == 0){
          fairShareRate = 0.toDouble
          nextT = Long.MaxValue
        } else {
          fairShareRate = 1 / activeJobNameList.size.toDouble
          logInfo("$$$$ $$$$ $$$$ smallest_F: %d".format(activeJobNameList(0)._2))
          nextT = realTime + (activeJobNameList(0)._2 - virtualTime) * activeJobNameList.size
          logInfo("$$$$ $$$$ $$$$ nextT: %d".format(nextT%1000000))
        }
      }
      logInfo("$$$$ $$$$ $$$$ handling: %s; %d %f".format(cname, virtualTime, fairShareRate))
      virtualTime = virtualTime + ((currentTime - realTime) * fairShareRate).toInt
      logInfo("$$$$ $$$$ $$$$ currentVirtualTime: %d".format(virtualTime))
      val currentJob = schedulableNameToSchedulable.get(cname)
      currentJob.GPSCompletionTime = virtualTime + currentJob.jobRunTime
      logInfo("$$$$ $$$$ $$$$ vCT: %d (jobRunTime: %d)"
        .format(currentJob.GPSCompletionTime, currentJob.jobRunTime))

      realTime = currentTime
      activeJobNameList.+=((cname, currentJob.GPSCompletionTime))
      fairShareRate = 1/activeJobNameList.size.toDouble
      activeJobNameList = activeJobNameList.sortWith(_._2 < _._2)
      logInfo("$$$$ $$$$ $$$$ smallest_F: %d".format(activeJobNameList(0)._2))
      nextT = currentTime + (activeJobNameList(0)._2 - virtualTime) * activeJobNameList.size
      logInfo("$$$$ $$$$ $$$$ nextT: %d".format(nextT%1000000))
      // udpate GPST and Event
    }
  }

  val CEvent = new Event()
//  var lastEvent:Event = {
//    if(schedulingMode == SchedulingMode.GPS){
//    }
//  }

//  def setGPSCompletionTime(): Unit = {
//    // only calculate GPSCT for GPS scheduling Mode
//    if (schedulingMode != SchedulingMode.GPS) {
//      return
//    }
//    val infEvent = new Event(Int.MaxValue)
//    val timeEventMap = scala.collection.mutable.Map(Int.MaxValue -> infEvent)
//    for (schedulable <- schedulableQueue.asScala) {
//      schedulable.remainingTime = schedulable.jobRunTime
//      if (timeEventMap.contains(schedulable.jobSubmittingTime)) {
//        timeEventMap(schedulable.jobSubmittingTime).addJob(schedulable.name)
//      }
//       else
//       {
//          timeEventMap(schedulable.jobSubmittingTime) = new Event(schedulable.jobSubmittingTime)
//          timeEventMap(schedulable.jobSubmittingTime).addJob(schedulable.name)
//       }
//    }
//    val timeEventBuffer = timeEventMap.toBuffer.sortWith(_._1 < _._1)
//    while (timeEventBuffer.size > 1){
//      val tmpEvent = timeEventBuffer.remove(0)._2
//      var nextFinishedJobName = ""
//      var nextFinishedJobTime = Int.MaxValue
//      for (jobName <- tmpEvent.activeJobNameQueue.asScala) {
//        val tmpJob = schedulableNameToSchedulable.get(jobName)
//        tmpJob.GPSCompletionTime = tmpEvent.eventTime +
//          tmpJob.remainingTime * tmpEvent.activeJobNameQueue.size()
//        if (tmpJob.GPSCompletionTime < nextFinishedJobTime) {
//          nextFinishedJobTime = tmpJob.GPSCompletionTime
//          nextFinishedJobName = tmpJob.name
//        }
//      }
//      if (nextFinishedJobTime > timeEventBuffer(0)._2.eventTime) {
//        val nextEvent = timeEventBuffer(0)._2
//        for (jobName <- tmpEvent.activeJobNameQueue.asScala) {
//          val tmpJob = schedulableNameToSchedulable.get(jobName)
//          tmpJob.remainingTime = tmpJob.remainingTime -
//            (nextEvent.eventTime - tmpEvent.eventTime) / tmpEvent.activeJobNameQueue.size()
//          nextEvent.addJob(tmpJob.name)
//        }
//      }
//      else {
//        val nextEvent = new Event(nextFinishedJobTime)
//        for (jobName <- tmpEvent.activeJobNameQueue.asScala) {
//          val tmpJob = schedulableNameToSchedulable.get(jobName)
//          if (tmpJob.GPSCompletionTime > nextFinishedJobTime) {
//            tmpJob.remainingTime = tmpJob.remainingTime -
//              (nextEvent.eventTime - tmpEvent.eventTime) / tmpEvent.activeJobNameQueue.size()
//            nextEvent.addJob(tmpJob.name)
//          } else {
//            tmpJob.remainingTime = 0
//            logInfo("##### ##### The GPSCompletionTime of Job %s : %d"
//              .format(tmpJob.name, tmpJob.GPSCompletionTime))
//          }
//        }
//        if (nextEvent.activeJobNameQueue.size > 0) {
//          timeEventBuffer.insert(0, (nextEvent.eventTime, nextEvent))
//        }
//      }
//    }
//  }

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  var name = poolName
  var parent: Pool = null

  // add by cc
  logInfo("##### ##### new pool created; Mode: %s".format(schedulingMode))
  var jobId = 0
  var jobSubmittingTime = 0
  var jobRunTime = 0
  var GPSCompletionTime = 0
  var jobRemainingRunTime = 0
  var taskRunTime = 0
//  var remainingTime = 0

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()

      // add by cc
      case SchedulingMode.GPS =>
        new GPSSchedulingAlgorithm()
      case SchedulingMode.SJF =>
        new SJFSchedulingAlgorithm()
    }
  }


  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this

    // add by cc
    if (schedulingMode == SchedulingMode.GPS){
      CEvent.addJob(schedulable.name)
    }
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)

    // add by cc
    // if a jobPool has no taskSetManager, then delete it from rootPool
    if (parent == null){
      return
    }
    if (parent.schedulingMode==SchedulingMode.GPS || parent.schedulingMode==SchedulingMode.SJF) {
      if (schedulableQueue.size() == 0) {
        parent.removeSchedulable(this)
        logInfo("##### ##### remove pool: %s".format(this.name))
      }
    }
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
   	var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
   	val sortedSchedulableQueue =
   	  schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
   	for (schedulable <- sortedSchedulableQueue) {
   	  sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
   	}
    if (schedulingMode == SchedulingMode.GPS) {
      for (taskSetManager <- sortedTaskSetQueue) {
        logInfo("##### ##### Print sortedResult in Queue: JobId-%d StId-%d tsID:%d tn-%d| GPSCT-%d"
          .format(taskSetManager.jobId, taskSetManager.stageId, taskSetManager.taskSet.stageId,
            taskSetManager.taskSet.tasks.size, taskSetManager.parent.GPSCompletionTime))
      }
      logInfo("##### ##### End printing in GPS")
    }
    if (schedulingMode == SchedulingMode.SJF) {
      for (taskSetManager <- sortedTaskSetQueue) {
        logInfo(("##### ##### Print sortedResult in Queue: JobId-%d StId-%d tsID:%d tn-%d| " +
          "jobRunTime-%d")
          .format(taskSetManager.jobId, taskSetManager.stageId, taskSetManager.taskSet.stageId,
            taskSetManager.taskSet.tasks.size, taskSetManager.parent.jobRunTime))
      }
      logInfo("##### ##### End printing in SJF")
    }
    if (schedulingMode == SchedulingMode.FAIR) {
      for (taskSetManager <- sortedTaskSetQueue) {
        logInfo("##### ##### Print sortedResult in Queue: JobId-%d StageId-%d | RunningTasks-%d"
          .format(taskSetManager.jobId, taskSetManager.stageId,
            taskSetManager.runningTasks))
      }
      logInfo("##### ##### End printing in FAIR")
    }
   	sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    jobRemainingRunTime -= taskRunTime
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
	// add by cc

  def readJobInfo(P: Int, S: String): (Int, Int, Int) = {
    // assert( S != null)
    var TmpS = S
    if (TmpS == null) {
      logWarning("##### ##### Invalid property: job-profiledInfo!")
      val filename = "/root/spark/job.profiledInfo"
      for (line <- Source.fromFile(filename).getLines()) {
        TmpS = line
      }
      if (TmpS == null){
        logWarning("##### ##### Invalid file: job.profiledInfo")
        return (0, 0, 0)
      }
    }
    val jobInfos = TmpS.split(' ')
    for ( jobInfo <- jobInfos) {
      val tmpJobInfo = jobInfo.split('+')
      if (tmpJobInfo.size < 4) {
        if (tmpJobInfo(0).toInt == P) {
          return (tmpJobInfo(1).toInt, tmpJobInfo(2).toInt, 0)
        }
      }
      if (tmpJobInfo(0).toInt == P){
        return (tmpJobInfo(1).toInt, tmpJobInfo(2).toInt, tmpJobInfo(3).toInt)
      }
    }
    logWarning("##### ##### No profiled properties for job_%d, read from File".format(P))
    (0, 0, 0)
  }

  override def setPoolProperty(P: Int, S: String){
    val thisJobInfo = readJobInfo(P, S)
    logInfo("##### ##### enter Pool's setPoolProperty: P: %d, JobSubmittingTime: %d, JobRunTime: %d"
      .format(P, thisJobInfo._1, thisJobInfo._2))
    jobId = P
    jobSubmittingTime = thisJobInfo._1
    jobRunTime = thisJobInfo._2
    taskRunTime = thisJobInfo._3
    jobRemainingRunTime = jobRunTime
  }
}
