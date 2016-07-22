package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSWrapper;

import java.io.*;

/**
 * Created by vagvaz on 03/07/16.
 */
public class KVSDescriptionResponse extends MCMessage{
  KVSConfiguration configuration;
  public static final String TYPE = "KVSDescriptionResponse";
  public KVSDescriptionResponse(KVSConfiguration configuration) {
    super(TYPE);
    this.configuration = configuration;
  }

  public KVSDescriptionResponse() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteArrayDataOutput output = ByteStreams.newDataOutput(bos);

    try {
      oos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ObjectInputStream ios = null;
    ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
    try {
      ios = new ObjectInputStream(bios);
      configuration = (KVSConfiguration) ios.readObject();
      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public KVSConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(KVSConfiguration configuration) {
    this.configuration = configuration;
  }
}
