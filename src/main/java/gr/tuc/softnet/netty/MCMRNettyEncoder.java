package gr.tuc.softnet.netty;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectDecoderInputStream;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * An encoder which serializes a Java object into a {@link ByteBuf}.
 * <p>
 * Please note that the serialized form this encoder produces is not
 * compatible with the standard {@link ObjectInputStream}.  Please use
 * {@link ObjectDecoder} or {@link ObjectDecoderInputStream} to ensure the
 * interoperability with this encoder.
 */
public class MCMRNettyEncoder extends MessageToByteEncoder<Serializable> {
  private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

  @Override
  protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {
    int startIdx = out.writerIndex();

    ByteBufOutputStream bout = new ByteBufOutputStream(out);
    bout.write(LENGTH_PLACEHOLDER);
    ObjectOutputStream oout = new CompactObjectOutputStream(bout);
    oout.writeObject(msg);
    oout.flush();
    oout.close();

    int endIdx = out.writerIndex();

    out.setInt(startIdx, endIdx - startIdx - 4);
  }
}
