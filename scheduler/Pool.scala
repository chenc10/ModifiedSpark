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
	var jobId = 0
	var jobSubmittingTime = 0
	var jobRunTime = 0
	var GPSCompletionTime = 0

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()

      // add by cc
			case SchedulingMode.GPS =>
				new GPSSchedulingAlgorithm()
			case SchedulingMode.LCP =>
				new LCPSchedulingAlgorithm()

    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
		
		// add by cc
		// if a jobPool has no taskSetManager, then delete it from rootPool
		if (schedulingMode == SchedulingMode.LCP && schedulableQueue.size() == 0){
			if (parent != null){
				parent.removeSchedulable(this)
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
		// add by cc, to be finished
		if(schedulingMode == SchedulingMode.GPS){
			// need to calculate the GPSCompletionTime first for GPS
			// also need to remove the jobpool within which all the tasks have finished
			for (schedulable <- schedulableQueue.asScala){
				schedulable.GPSCompletionTime = schedulable.jobSubmittingTime + schedulable.jobRunTime
			}
		}
   	val sortedSchedulableQueue =
   	  schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
   	for (schedulable <- sortedSchedulableQueue) {
   	  sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
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
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
	// add by cc
  def setPoolProperty(priority: Int, JobSubmittingTime: Int, JobRunTime: Int){
		jobId = priority
		jobSubmittingTime = JobSubmittingTime
		jobRunTime = JobRunTime
  }
}
