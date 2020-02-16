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
package org.apache.dolphinscheduler.server.worker.task.conditions;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.DependResult;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.task.AbstractParameters;
import org.apache.dolphinscheduler.common.task.dependent.DependentParameters;
import org.apache.dolphinscheduler.common.utils.DependentUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.worker.task.AbstractTask;
import org.apache.dolphinscheduler.server.worker.task.TaskProps;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConditionsTask extends AbstractTask {


    /**
     * dependent parameters
     */
    private DependentParameters dependentParameters;

    /**
     * process dao
     */
    private ProcessDao processDao;

    /**
     * taskInstance
     */
    private TaskInstance taskInstance;

    /**
     * processInstance
     */
    private ProcessInstance processInstance;

    /**
     *
     */
    private Map<String, ExecutionStatus> completeTaskList = new ConcurrentHashMap<>();

    /**
     * constructor
     *
     * @param taskProps task props
     * @param logger    logger
     */
    public ConditionsTask(TaskProps taskProps, Logger logger) {
        super(taskProps, logger);
    }

    @Override
    public void init() throws Exception {
        logger.info("conditions task initialize");

        this.dependentParameters = JSONUtils.parseObject(this.taskProps.getDependence(), DependentParameters.class);

        this.taskInstance = processDao.findTaskInstanceById(taskProps.getTaskInstId());

        if(taskInstance == null){
            throw new Exception("cannot find the task instance!");
        }

        List<TaskInstance> taskInstanceList = processDao.findValidTaskListByProcessId(taskInstance.getProcessInstanceId());
        for(TaskInstance task : taskInstanceList){
            this.completeTaskList.putIfAbsent(task.getName(), task.getState());
        }
    }

    @Override
    public void handle() throws Exception {
        List<DependResult> modelResultList = new ArrayList<>();
        for(DependentTaskModel dependentTaskModel : dependentParameters.getDependTaskList()){

            List<DependResult> itemDependResult = new ArrayList<>();
            for(DependentItem item : dependentTaskModel.getDependItemList()){
                itemDependResult.add(getDependResultForItem(item));
            }
            DependResult modelResult = DependentUtils.getDependResultForRelation(dependentTaskModel.getRelation(), itemDependResult);
            modelResultList.add(modelResult);
        }
        DependResult result = DependentUtils.getDependResultForRelation(
                dependentParameters.getRelation(), modelResultList
        );
        exitStatusCode = (result == DependResult.SUCCESS) ?
                Constants.EXIT_CODE_SUCCESS : Constants.EXIT_CODE_FAILURE;
    }

    private DependResult getDependResultForItem(DependentItem item){

        if(!completeTaskList.containsKey(item.getDepTasks())){
            return DependResult.FAILED;
        }
        ExecutionStatus executionStatus = completeTaskList.get(item.getDepTasks());
        if(executionStatus != item.getStatus()){
            return DependResult.FAILED;
        }
        return DependResult.SUCCESS;
    }

    @Override
    public AbstractParameters getParameters() {
        return null;
    }
}
