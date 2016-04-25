package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

/**
 * Created by vagvaz on 1/04/16.
 */
public class KVSDestroy extends MCMessage {
  public static final String TYPE = "KVSDestroy";

  String kvsName;


  public  KVSDestroy(){
    super(TYPE);
  }

  public KVSDestroy(String kvsName){
    super(TYPE);
    this.kvsName = kvsName;
  }



  @Override public byte[] toBytes() {

    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    output.writeUTF(kvsName);
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    kvsName = input.readUTF();
  }
}
