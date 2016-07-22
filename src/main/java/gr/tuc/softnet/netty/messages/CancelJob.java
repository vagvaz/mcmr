package gr.tuc.softnet.netty.messages;

import gr.tuc.softnet.core.StringConstants;

/**
 * Created by vagvaz on 17/05/16.
 */
public class CancelJob extends MCMessage {
  public static final String TYPE = StringConstants.CANCEL_JOB;
  String id;
  public CancelJob(String id) {
    super(TYPE);
    this.id = id;
  }

  public CancelJob() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    return new byte[0];
  }

  @Override public void fromBytes(byte[] bytes) {

  }
}
