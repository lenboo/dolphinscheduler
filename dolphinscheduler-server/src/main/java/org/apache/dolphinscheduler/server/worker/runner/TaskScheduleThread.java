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


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.sift.SiftingAppender;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.TaskTimeoutParameter;
import org.apache.dolphinscheduler.common.utils.*;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.server.utils.LoggerUtils;
import org.apache.dolphinscheduler.server.worker.log.TaskLogDiscriminator;
import org.apache.dolphinscheduler.server.worker.task.AbstractTask;
import org.apache.dolphinscheduler.server.worker.task.TaskManager;
import org.apache.dolphinscheduler.server.worker.task.TaskProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 *  task scheduler thread
 */
public class TaskScheduleThread implements Runnable {

    /**
     * logger
     */
    private final Logger logger = LoggerFactory.getLogger(TaskScheduleThread.class);

    /**
     *  task instance
     */
    private TaskInstance taskInstance;

    /**
     *  process database access
     */
    private final ProcessDao processDao;

    /**
     *  abstract task
     */
    private AbstractTask task;

    /**
     * task instance id
     */
    private int taskInstanceId;

    /**
     * constructor
     *
     * @param taskInstanceId  task instance id
     * @param processDao    process dao
     */
    public TaskScheduleThread(int taskInstanceId, ProcessDao processDao){
        this.processDao = processDao;
        this.taskInstanceId = taskInstanceId;
    }


    private boolean initTaskInstance() throws IOException {

        this.taskInstance.setStartTime(new Date());
        this.taskInstance.setHost(OSUtils.getHost());

        this.taskInstance = processDao.getTaskInstanceDetailByTaskId(taskInstanceId);
        // if process definition is null ,process definition already deleted
        int userId = taskInstance.getProcessDefine() == null ? 0 : taskInstance.getProcessDefine().getUserId();

        Tenant tenant = processDao.getTenantForProcess(
                taskInstance.getProcessInstance().getTenantId(),
                userId);

        // verify tenant is null
        if (tenant == null) {
            processDao.changeTaskState(ExecutionStatus.FAILURE,
                        taskInstance.getStartTime(),
                        taskInstance.getHost(),
                        null,
                        null,
                        taskInstance.getId());
            logger.warn("task id:{} name:{} run failed due to tenant is null", taskInstance.getId(), taskInstance.getName());
            return false;
        }

        // set queue for process instance, user-specified queue takes precedence over tenant queue
        String userQueue = processDao.queryUserQueueByProcessInstanceId(taskInstance.getProcessInstanceId());
        taskInstance.getProcessInstance().setQueue(StringUtils.isEmpty(userQueue) ? tenant.getQueue() : userQueue);
        taskInstance.getProcessInstance().setTenantCode(tenant.getTenantCode());

        // local execute path
        String execLocalPath = getExecLocalPath();
        this.taskInstance.setExecutePath(execLocalPath);
        logger.info("task instance  local execute path : {} ", execLocalPath);
        // check and create Linux users
        FileUtils.createWorkDirAndUserIfAbsent(execLocalPath,
                tenant.getTenantCode(), logger);
        return true;
    }

    @Override
    public void run() {

        try {

            if(!initTaskInstance()){
                return;
            }
            // update task state is running according to task type
            updateTaskState(taskInstance.getTaskType());

            logger.info("script path : {}", taskInstance.getExecutePath());
            // task node
            TaskNode taskNode = JSONObject.parseObject(taskInstance.getTaskJson(), TaskNode.class);

            // copy hdfs/minio file to local
            copyHdfsToLocal(processDao,
                    taskInstance.getExecutePath(),
                    createProjectResFiles(taskNode),
                    logger);

            // get process instance according to tak instance
            ProcessInstance processInstance = taskInstance.getProcessInstance();

            // set task props
            TaskProps taskProps = new TaskProps(taskNode.getParams(),
                    taskInstance.getExecutePath(),
                    processInstance.getScheduleTime(),
                    taskInstance.getName(),
                    taskInstance.getTaskType(),
                    taskInstance.getId(),
                    CommonUtils.getSystemEnvPath(),
                    processInstance.getTenantCode(),
                    processInstance.getQueue(),
                    taskInstance.getStartTime(),
                    getGlobalParamsMap(),
                    taskInstance.getDependency(),
                    processInstance.getCmdTypeIfComplement());
            // set task timeout
            setTaskTimeout(taskProps, taskNode);

            taskProps.setTaskAppId(String.format("%s_%s_%s",
                    taskInstance.getProcessDefine().getId(),
                    taskInstance.getProcessInstance().getId(),
                    taskInstance.getId()));

            // custom logger
            Logger taskLogger = LoggerFactory.getLogger(LoggerUtils.buildTaskId(LoggerUtils.TASK_LOGGER_INFO_PREFIX,
                    taskInstance.getProcessDefine().getId(),
                    taskInstance.getProcessInstance().getId(),
                    taskInstance.getId()));

            task = TaskManager.newTask(taskInstance.getTaskType(),
                    taskProps,
                    taskLogger);

            // task init
            task.init();

            // task handle
            task.handle();

            // task result process
            task.after();

        }catch (Exception e){
            logger.error("task scheduler failure", e);
            kill();
            // update task instance state
            processDao.changeTaskState(ExecutionStatus.FAILURE,
                    new Date(),
                    taskInstance.getId());
        }

        logger.info("task instance id : {},task final status : {}",
                taskInstance.getId(),
                task.getExitStatus());
        // update task instance state
        processDao.changeTaskState(task.getExitStatus(),
                new Date(),
                taskInstance.getId());
    }

    /**
     * get global paras map
     * @return
     */
    private Map<String, String> getGlobalParamsMap() {
        Map<String,String> globalParamsMap = new HashMap<>(16);

        // global params string
        String globalParamsStr = taskInstance.getProcessInstance().getGlobalParams();

        if (globalParamsStr != null) {
            List<Property> globalParamsList = JSONObject.parseArray(globalParamsStr, Property.class);
            globalParamsMap.putAll(globalParamsList.stream().collect(Collectors.toMap(Property::getProp, Property::getValue)));
        }
        return globalParamsMap;
    }

    /**
     *  update task state according to task type
     * @param taskType
     */
    private void updateTaskState(String taskType) {

        String excutePath = getTaskLogPath();
        if(taskType.equals(TaskType.SQL.name())  ||
                    taskType.equals(TaskType.PROCEDURE.name())){
            excutePath = null;
        }
        processDao.changeTaskState(ExecutionStatus.RUNNING_EXEUTION,
                taskInstance.getStartTime(),
                taskInstance.getHost(),
                excutePath,
                getTaskLogPath(),
                taskInstance.getId());
    }

    /**
     *  get task log path
     * @return
     */
    private String getTaskLogPath() {
        String logPath;
        try{
            String baseLog = ((TaskLogDiscriminator) ((SiftingAppender) ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger("ROOT")
                .getAppender("TASKLOGFILE"))
                .getDiscriminator()).getLogBase();
        if (baseLog.startsWith(Constants.SINGLE_SLASH)){
            logPath =  baseLog + Constants.SINGLE_SLASH +
                    taskInstance.getProcessDefinitionId() + Constants.SINGLE_SLASH  +
                    taskInstance.getProcessInstanceId() + Constants.SINGLE_SLASH  +
                    taskInstance.getId() + ".log";
        }else{
            logPath = System.getProperty("user.dir") + Constants.SINGLE_SLASH +
                baseLog +  Constants.SINGLE_SLASH +
                taskInstance.getProcessDefinitionId() + Constants.SINGLE_SLASH  +
                taskInstance.getProcessInstanceId() + Constants.SINGLE_SLASH  +
                taskInstance.getId() + ".log";
        }
        }catch (Exception e){
            logger.error("logger" + e);
            logPath = "";
        }
        return logPath;
    }

    /**
     * set task timeout
     * @param taskProps
     * @param taskNode
     */
    private void setTaskTimeout(TaskProps taskProps, TaskNode taskNode) {
        // the default timeout is the maximum value of the integer
        taskProps.setTaskTimeout(Integer.MAX_VALUE);
        TaskTimeoutParameter taskTimeoutParameter = taskNode.getTaskTimeoutParameter();
        if (taskTimeoutParameter.getEnable()){
            // get timeout strategy
            taskProps.setTaskTimeoutStrategy(taskTimeoutParameter.getStrategy());
            switch (taskTimeoutParameter.getStrategy()){
                case WARN:
                    break;
                case FAILED:
                    if (Integer.MAX_VALUE > taskTimeoutParameter.getInterval() * 60) {
                        taskProps.setTaskTimeout(taskTimeoutParameter.getInterval() * 60);
                    }
                    break;
                case WARNFAILED:
                    if (Integer.MAX_VALUE > taskTimeoutParameter.getInterval() * 60) {
                        taskProps.setTaskTimeout(taskTimeoutParameter.getInterval() * 60);
                    }
                    break;
                default:
                    logger.error("not support task timeout strategy: {}", taskTimeoutParameter.getStrategy());
                    throw new IllegalArgumentException("not support task timeout strategy");

            }
        }
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
     *  kill task
     */
    public void kill(){
        if (task != null){
            try {
                task.cancelApplication(true);
            }catch (Exception e){
                logger.error(e.getMessage(),e);
            }
        }
    }


    /**
     *  create project resource files
     */
    private List<String> createProjectResFiles(TaskNode taskNode) throws Exception{

        Set<String> projectFiles = new HashSet<>();
        AbstractParameters baseParam = TaskParametersUtils.getParameters(taskNode.getType(), taskNode.getParams());

        if (baseParam != null) {
            List<String> projectResourceFiles = baseParam.getResourceFilesList();
            if (projectResourceFiles != null) {
                projectFiles.addAll(projectResourceFiles);
            }
        }

        return new ArrayList<>(projectFiles);
    }

    /**
     * copy hdfs file to local
     *
     * @param processDao
     * @param execLocalPath
     * @param projectRes
     * @param logger
     */
    private void copyHdfsToLocal(ProcessDao processDao, String execLocalPath, List<String> projectRes, Logger logger) throws IOException {
        for (String res : projectRes) {
            File resFile = new File(execLocalPath, res);
            if (!resFile.exists()) {
                try {
                    // query the tenant code of the resource according to the name of the resource
                    String tentnCode = processDao.queryTenantCodeByResName(res);
                    String resHdfsPath = HadoopUtils.getHdfsFilename(tentnCode, res);

                    logger.info("get resource file from hdfs :{}", resHdfsPath);
                    HadoopUtils.getInstance().copyHdfsToLocal(resHdfsPath, execLocalPath + File.separator + res, false, true);
                }catch (Exception e){
                    logger.error(e.getMessage(),e);
                    throw new RuntimeException(e.getMessage());
                }
            } else {
                logger.info("file : {} exists ", resFile.getName());
            }
        }
    }
}