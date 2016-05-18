package gr.tuc.softnet.netty;

import java.io.Serializable;

/**
 * Created by vagvaz on 11/25/15.
 */
public class AcknowledgeMessage implements Serializable {
  private long ackMessageId;

  public AcknowledgeMessage(long l) {
    this.ackMessageId = l;
  }

  public long getAckMessageId() {
    return ackMessageId;
  }

  public void setAckMessageId(long ackMessageId) {
    this.ackMessageId = ackMessageId;
  }

}
