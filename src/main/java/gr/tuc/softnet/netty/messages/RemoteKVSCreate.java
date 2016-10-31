package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.kvs.KVSConfiguration;

import java.io.*;

/**
 * Created by vagvaz on 1/04/16.
 */
public class RemoteKVSCreate extends MCMessage {
  public static final String TYPE = StringConstants.KVS_REMOTE_KVS;

  String kvsName;
  KVSConfiguration configuration;

  public RemoteKVSCreate(){
    super(TYPE);
  }

  public RemoteKVSCreate(String kvsName, KVSConfiguration configuration){
    super(TYPE);
    this.kvsName = kvsName;
    this.configuration = configuration;
  }



  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(kvsName);
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
      kvsName = (String) ios.readObject();
      configuration = (KVSConfiguration) ios.readObject();
      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String getKvsName() {
    return kvsName;
  }

  public void setKvsName(String kvsName) {
    this.kvsName = kvsName;
  }

  public KVSConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(KVSConfiguration configuration) {
    this.configuration = configuration;
  }
}
