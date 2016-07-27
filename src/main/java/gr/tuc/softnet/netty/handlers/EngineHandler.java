package gr.tuc.softnet.netty.handlers;

import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.netty.MCMessageHandler;
import gr.tuc.softnet.netty.messages.*;
import io.netty.channel.Channel;
import rx.Observable;

/**
 * Created by vagvaz on 18/05/16.
 */
public class EngineHandler extends MCMessageHandler {
  public EngineHandler(){
    super();
  }

  @Override public MCMessageWrapper process(Channel node, MCMessageWrapper wrapper) {
    switch(wrapper.getType()){
      case StringConstants.SUBMIT_JOB: {
        SubmitJob message = (SubmitJob) wrapper.getMessage();
        message.getConfiguration().setClient(message.getClient());
        message.getConfiguration().setProperty(StringConstants.REQUEST_NUMBER, wrapper.getRequestId());
        this.jobManager.startJob(message.getConfiguration());
        break;
      }case StringConstants.TASK_COMPLETED: {
        TaskCompleted message = (TaskCompleted) wrapper.getMessage();
        TaskConfiguration task = message.getTask();
        this.jobManager.taskCompleted(task.getJobID(), task.getTargetCloud(), task.getID());
      }case StringConstants.START_TASK:{
        StartTask message = (StartTask)wrapper.getMessage();
        TaskConfiguration conf = message.getConf();
        this.taskManager.startTask(conf);
        break;
      }case StringConstants.JOB_COMPLETED:{
        JobCompleted message = (JobCompleted)wrapper.getMessage();
        this.jobManager.completedJob(message.getConf());
      }
    }
    return null;
  }

}
