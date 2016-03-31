package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.mapred.TaskState;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.Serializable;

/**
 * Created by vagvaz on 10/02/16.
 */
public class TaskStatus implements Statusable,IDable,Serializable {

    String taskID;
    HierarchicalConfiguration configuration;
    volatile MCTask task;
    SummaryStatistics readStatistics;
    SummaryStatistics processedStatistics;
    SummaryStatistics writeStatistics;
    private Throwable exception;
    private Throwable errorException =null;
    private TaskState state;

    public TaskStatus(TaskConfiguration taskConf){
        taskID = taskConf.getID();
        configuration = new HierarchicalConfiguration();
        configuration.append(taskConf);
        readStatistics = new SummaryStatistics();
        processedStatistics = new SummaryStatistics();
        writeStatistics = new SummaryStatistics();
    }
    public void addReadCounter(Number read){
        readStatistics.addValue(read.doubleValue());
    }

    public void addProcessedCounter(Number read){
        processedStatistics.addValue(read.doubleValue());
    }
    public void addWriteCounter(Number read){
        writeStatistics.addValue(read.doubleValue());
    }
    public String getID() {
        return taskID;
    }

    public MCConfiguration getConfiguration() {
        if(task != null) {
            return task.getConfiguration();
        }else{
            return null;
        }
    }

    public Configuration getStatus() {
        configuration.addProperty("read",readStatistics);
        configuration.addProperty("processed",processedStatistics);
        configuration.addProperty("write",writeStatistics);
        return configuration;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public void setConfiguration(HierarchicalConfiguration configuration) {
        this.configuration = configuration;
    }

    public MCTask getTask() {
        return task;
    }

    public void setTask(MCTask task) {
        this.task = task;
    }

    public Throwable getException() {
        return exception;
    }
    public void setException(Throwable e){
        if(this.errorException == null) {
            this.errorException = e;
        }
    }

    public TaskState getState() {
        return state;
    }
    public void getState(TaskState state){
        this.state = state;
    }

    public void setMessage(String msg) {
        configuration.setProperty("message",msg);
    }
    public String getMessage(){
        return  configuration.getString("message");
    }
}
