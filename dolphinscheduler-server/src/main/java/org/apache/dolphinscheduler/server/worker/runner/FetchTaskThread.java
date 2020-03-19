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
package org.apache.dolphinscheduler.server.worker.runner;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.queue.ITaskQueue;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.*;
import org.apache.dolphinscheduler.common.zk.AbstractZKClient;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.server.zk.ZKWorkerClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *  fetch task thread
 */
public class FetchTaskThread implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(FetchTaskThread.class);
    /**
     *  set worker concurrent tasks
     */
    private final int taskNum;

    /**
     *  zkWorkerClient
     */
    private final ZKWorkerClient zkWorkerClient;

    /**
     * task queue impl
     */
    protected ITaskQueue taskQueue;

    /**
     *  process database access
     */
    private final ProcessDao processDao;

    /**
     *  worker thread pool executor
     */
    private final ExecutorService workerExecService;

    /**
     *  worker exec nums
     */
    private int workerExecNums;

    /**
     * conf
     */
    private Configuration conf;

    /**
     *  task instance
     */
    private TaskInstance taskInstance;

    /**
     * task instance id
     */
    Integer taskInstId;

    public FetchTaskThread(int taskNum, ZKWorkerClient zkWorkerClient,
                           ProcessDao processDao, Configuration conf,
                           ITaskQueue taskQueue){
        this.taskNum = taskNum;
        this.zkWorkerClient = zkWorkerClient;
        this.processDao = processDao;
        this.workerExecNums = conf.getInt(Constants.WORKER_EXEC_THREADS,
                Constants.defaultWorkerExecThreadNum);
        // worker thread pool executor
        this.workerExecService = ThreadUtils.newDaemonFixedThreadExecutor("Worker-Fetch-Task-Thread",workerExecNums);
        this.conf = conf;
        this.taskQueue = taskQueue;
        this.taskInstance = null;
    }


    /**
     * Check if the task runs on this worker
     * @param taskQueueStr
     * @return
     */
    private boolean checkWorkerGroup(String taskQueueStr, String hostNumber) {

        String[] taskQueueArray = taskQueueStr.split(Constants.UNDERLINE);
        if(taskQueueArray.length < 5){
            logger.warn("there is no worker group information, wrong format task queue array :" + taskQueueStr);
            return true;
        }
        String hostListStr = taskQueueArray[4];
        if(hostListStr.equals(String.valueOf(Constants.DEFAULT_WORKER_ID))){
            return true;
        }
        return Arrays.asList(hostListStr.split(Constants.COMMA)).contains(hostNumber);
    }

    /**
     * Check if the task runs on this worker
     * @param taskInstance
     * @param host
     * @return
     */
    private boolean checkWorkerGroup(TaskInstance taskInstance, String host){

        int taskWorkerGroupId = processDao.getTaskWorkerGroupId(taskInstance);

        if(taskWorkerGroupId <= 0){
            return true;
        }
        WorkerGroup workerGroup = processDao.queryWorkerGroupById(taskWorkerGroupId);
        if(workerGroup == null ){
            logger.info("task {} cannot find the worker group, use all worker instead.", taskInstance.getId());
            return true;
        }
        String ips = workerGroup.getIpList();
        if(StringUtils.isBlank(ips)){
            logger.error("task:{} worker group:{} parameters(ip_list) is null, this task would be running on all workers",
                    taskInstance.getId(), workerGroup.getId());
        }
        String[] ipArray = ips.split(Constants.COMMA);
        List<String> ipList =  Arrays.asList(ipArray);
        return ipList.contains(host);
    }

    private boolean executeTaskInstance(String taskQueueStr) throws IOException {

        // get task instance id
        taskInstId = getTaskInstanceId(taskQueueStr);

        taskInstance = processDao.getTaskInstanceDetailByTaskId(taskInstId);

        // verify task instance is null
        if (verifyTaskInstanceIsNull(taskInstance)) {
            logger.warn("remove task queue : {} due to taskInstance is null", taskQueueStr);
            return false;
        }

        // if process definition is null ,process definition already deleted
        int userId = taskInstance.getProcessDefine() == null ? 0 : taskInstance.getProcessDefine().getUserId();

        Tenant tenant = processDao.getTenantForProcess(
                taskInstance.getProcessInstance().getTenantId(),
                userId);

        // verify tenant is null
        if (verifyTenantIsNull(tenant)) {
            logger.warn("remove task queue : {} due to tenant is null", taskQueueStr);
            processErrorTask(taskQueueStr);
            return false;
        }

        // set queue for process instance, user-specified queue takes precedence over tenant queue
        String userQueue = processDao.queryUserQueueByProcessInstanceId(taskInstance.getProcessInstanceId());
        taskInstance.getProcessInstance().setQueue(StringUtils.isEmpty(userQueue) ? tenant.getQueue() : userQueue);
        taskInstance.getProcessInstance().setTenantCode(tenant.getTenantCode());

        logger.info("worker fetch taskId : {} from queue ", taskInstId);

        // local execute path
        String execLocalPath = getExecLocalPath();

        logger.info("task instance  local execute path : {} ", execLocalPath);

        // init task
        taskInstance.init(OSUtils.getHost(),
                new Date(),
                execLocalPath);

        // check and create Linux users
        FileUtils.createWorkDirAndUserIfAbsent(execLocalPath,
                tenant.getTenantCode(), logger);

        logger.info("task : {} ready to submit to task scheduler thread",taskInstId);
        // submit task
        workerExecService.submit(new TaskScheduleThread(taskInstance, processDao));
        return  true;
    }



    @Override
    public void run() {
        while (Stopper.isRunning()){
            InterProcessMutex mutex = null;
            String currentTaskQueueStr = null;

            String hostNumber = IpUtils.ipToLong(OSUtils.getHost()).toString();
            try {
                ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) workerExecService;
                //check memory and cpu usage and threads
                boolean runCheckFlag = OSUtils.checkResource(this.conf, false)
                        && checkThreadCount(poolExecutor);
                Thread.sleep(Constants.SLEEP_TIME_MILLIS);

                if(!runCheckFlag) {
                    continue;
                }

                //whether have tasks, if no tasks , no need lock  //get all tasks
                List<String> tasksQueueList = taskQueue.getAllTasks(
                        Constants.DOLPHINSCHEDULER_TASKS_QUEUE);
                if (CollectionUtils.isEmpty(tasksQueueList)){
                    continue;
                }
                // creating distributed locks, lock path /dolphinscheduler/lock/worker
                mutex = zkWorkerClient.acquireZkLock(zkWorkerClient.getZkClient(),
                        zkWorkerClient.getWorkerLockPath());


                // task instance id str
                List<String> taskQueueStrArr = taskQueue.poll(
                        Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskNum);

                for(String taskQueueStr : taskQueueStrArr){

                    currentTaskQueueStr = taskQueueStr;
                    if (StringUtils.isEmpty(taskQueueStr)) {
                        continue;
                    }

                    if (!checkThreadCount(poolExecutor)) {
                        break;
                    }
                    if(!checkWorkerGroup(taskQueueStr, hostNumber)){
                        continue;
                    }

                    executeTaskInstance(taskQueueStr);

                    // remove node from zk
                    removeNodeFromTaskQueue(taskQueueStr);
                }

            }catch (Exception e){
                processErrorTask(currentTaskQueueStr);
                logger.error("fetch task thread failure" ,e);
            }finally {
                AbstractZKClient.releaseMutex(mutex);
            }
        }
    }


    /**
     * process error task
     * @param taskQueueStr task queue str
     */
    private void processErrorTask(String taskQueueStr){
        // remove from zk
        removeNodeFromTaskQueue(taskQueueStr);

        if (taskInstance != null){
            processDao.changeTaskState(ExecutionStatus.FAILURE,
                    taskInstance.getStartTime(),
                    taskInstance.getHost(),
                    null,
                    null,
                    taskInstId);
        }

    }
    /**
     * remove node from task queue
     *
     * @param taskQueueStr task queue
     */
    private void removeNodeFromTaskQueue(String taskQueueStr){
        taskQueue.removeNode(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskQueueStr);
    }

    /**
     * verify task instance is null
     * @param taskInstance
     * @return true if task instance is null
     */
    private boolean verifyTaskInstanceIsNull(TaskInstance taskInstance) {
        if (taskInstance == null ) {
            logger.error("task instance is null. task id : {} ", taskInstId);
            return true;
        }
        return false;
    }

    /**
     * verify tenant is null
     *
     * @param tenant tenant
     * @return true if tenant is null
     */
    private Boolean verifyTenantIsNull(Tenant tenant) {
        if(tenant == null){
            logger.error("tenant not exists,process instance id : {},task instance id : {}",
                    taskInstance.getProcessInstance().getId(),
                    taskInstance.getId());
            return true;
        }
        return false;
    }

    /**
     * get execute local path
     *
     * @return execute local path
     */
    private String getExecLocalPath(){
        return FileUtils.getProcessExecDir(taskInstance.getProcessDefine().getProjectId(),
                taskInstance.getProcessDefine().getId(),
                taskInstance.getProcessInstance().getId(),
                taskInstance.getId());
    }

    /**
     * check thread count
     *
     * @param poolExecutor pool executor
     * @return true if active count < worker exec nums
     */
    private boolean checkThreadCount(ThreadPoolExecutor poolExecutor) {
        int activeCount = poolExecutor.getActiveCount();
        if (activeCount >= workerExecNums) {
            logger.info("thread insufficient , activeCount : {} , " +
                            "workerExecNums : {}, will sleep : {} millis for thread resource",
                    activeCount,
                    workerExecNums,
                    Constants.SLEEP_TIME_MILLIS);
            return false;
        }
        return true;
    }

    /**
     * wait for task instance exists, because of db action would be delayed.
     *
     * @throws Exception exception
     */
    private void waitForTaskInstance()throws Exception{
        int retryTimes = 30;
        while (Stopper.isRunning()) {
            taskInstance = processDao.findTaskInstanceById(taskInstId);
            if(taskInstance != null || retryTimes <= 0){
                break;
            }
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
            retryTimes--;
        }
    }

    /**
     * get task instance id
     *
     * @param taskQueueStr task queue
     * @return task instance id
     */
    private int getTaskInstanceId(String taskQueueStr){
        return Integer.parseInt(taskQueueStr.split(Constants.UNDERLINE)[3]);
    }
}