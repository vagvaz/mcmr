package gr.tuc.softnet.netty.handlers;

import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.netty.MCMessageHandler;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import gr.tuc.softnet.netty.messages.SubmitJob;
import io.netty.channel.Channel;

/**
 * Created by vagvaz on 18/05/16.
 */
public class EngineHandler extends MCMessageHandler {
  public EngineHandler(){
    super();
  }

  @Override public MCMessageWrapper process(Channel node, MCMessageWrapper wrapper) {
    switch(wrapper.getType()){
      case StringConstants.SUBMIT_JOB:
        SubmitJob message = (SubmitJob) wrapper.getMessage();
        message.getConfiguration().setClient(message.getClient());
        this.jobManager.startJob(message.getConfiguration());
        break;
    }
    return null;
  }

}
