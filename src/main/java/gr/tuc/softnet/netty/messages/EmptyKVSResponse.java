package gr.tuc.softnet.netty.messages;

import gr.tuc.softnet.core.StringConstants;

/**
 * Created by vagvaz on 25/09/16.
 */
public class EmptyKVSResponse extends MCMessage {
  /**
   * Created by vagvaz on 26/08/16.
   */
  public final static String TYPE = StringConstants.KVSEMPTYRESPONSE;

  public EmptyKVSResponse() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    return new byte[0];
  }

  @Override public void fromBytes(byte[] bytes) {
  }
}

