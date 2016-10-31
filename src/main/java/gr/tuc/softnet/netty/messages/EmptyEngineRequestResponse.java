package gr.tuc.softnet.netty.messages;

import gr.tuc.softnet.core.StringConstants;

/**
 * Created by vagvaz on 26/08/16.
 */
public class EmptyEngineRequestResponse extends MCMessage {
  public final static String TYPE = StringConstants.ENGEMPTYRESPONSE;

  public EmptyEngineRequestResponse() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    return new byte[0];
  }

  @Override public void fromBytes(byte[] bytes) {
  }
}
