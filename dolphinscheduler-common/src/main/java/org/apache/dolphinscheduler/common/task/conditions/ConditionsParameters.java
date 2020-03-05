package org.apache.dolphinscheduler.common.task.conditions;

import org.apache.dolphinscheduler.common.enums.DependentRelation;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.task.AbstractParameters;

import java.util.List;

public class ConditionsParameters extends AbstractParameters {

    //depend node list and state, only need task name
    private List<DependentTaskModel> dependTaskList;
    private DependentRelation dependRelation;

    // node list to run when success
    private List<String> successNode;

    // node list to run when failed
    private List<String> failedNode;


    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<String> getResourceFilesList() {
        return null;
    }

    public List<DependentTaskModel> getDependTaskList() {
        return dependTaskList;
    }

    public void setDependTaskList(List<DependentTaskModel> dependTaskList) {
        this.dependTaskList = dependTaskList;
    }

    public DependentRelation getDependRelation() {
        return dependRelation;
    }

    public void setDependRelation(DependentRelation dependRelation) {
        this.dependRelation = dependRelation;
    }

    public List<String> getSuccessNode() {
        return successNode;
    }

    public void setSuccessNode(List<String> successNode) {
        this.successNode = successNode;
    }

    public List<String> getFailedNode() {
        return failedNode;
    }

    public void setFailedNode(List<String> failedNode) {
        this.failedNode = failedNode;
    }
}
