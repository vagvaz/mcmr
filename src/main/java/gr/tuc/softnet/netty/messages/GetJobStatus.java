package gr.tuc.softnet.netty.messages;

import gr.tuc.softnet.core.StringConstants;

/**
 * Created by vagvaz on 17/05/16.
 */
public class GetJobStatus extends MCMessage {
  public static final String TYPE = StringConstants.GET_JOB_STATUS;
  public GetJobStatus(String messageType) {
    super(TYPE);
  }

  public GetJobStatus() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    return new byte[0];
  }

  @Override public void fromBytes(byte[] bytes) {

  }
}
