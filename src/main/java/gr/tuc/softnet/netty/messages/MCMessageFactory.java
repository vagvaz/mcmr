package gr.tuc.softnet.netty.messages;

/**
 * Created by vagvaz on 1/04/16.
 */
public class MCMessageFactory {
  public static MCMessage getMessage(String type) {
    switch(type){
      case NoMoreInputMessage.TYPE:
        return new NoMoreInputMessage();
      case SubmitJob.TYPE:
        return new SubmitJob();
      case CancelJob.TYPE:
        return new CancelJob();
      case GetJobStatus.TYPE:
        return new GetJobStatus();
      case JobCompleted.TYPE:
        return new JobCompleted();
      case KillNode.TYPE:
        return new KillNode();
      case KVSBatchPut.TYPE:
        return new KVSBatchPut();
      case KVSContains.TYPE:
        return new KVSContains();
      case KVSDescriptionRequest.TYPE:
        return new KVSDescriptionRequest();
      case KVSDescriptionResponse.TYPE:
        return new KVSDescriptionResponse();
      case KVSDestroy.TYPE:
        return new KVSDestroy();
      case KVSGet.TYPE:
        return new KVSGet();
      case KVSGetResponse.TYPE:
        return new KVSGetResponse();
      case KVSPut.TYPE:
        return new KVSPut();
      case NodeAnnouncementMessage.TYPE:
        return new NodeAnnouncementMessage();
      case ResetNode.TYPE:
        return new ResetNode();
      case StartTask.TYPE:
        return new StartTask();
      case TaskCompleted.TYPE:
        return new TaskCompleted();
      default:
        return null;
    }
  }
}
