package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * Created by vagvaz on 1/04/16.
 */
public class KVSContains<K extends WritableComparable> extends MCMessage{
  public static final String TYPE = "KVSContains";
  private String kvsname;
  Class<K> keyClass;
  K key;
  private static Configuration conf = new Configuration();

  public  KVSContains(){
    super(TYPE);
  }

  public KVSContains(String kvsName, K key, Class<K> keyClass){
    super(TYPE);
    this.key = key;
    this.keyClass = keyClass;
    this.kvsname = kvsName;
  }



  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(keyClass);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteArrayDataOutput output = ByteStreams.newDataOutput(bos);
    try {
      key.write(output);
      output.writeUTF(kvsname);
    } catch (IOException e) {
      e.printStackTrace();
    }
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
      keyClass = (Class<K>) ios.readObject();
      key = ReflectionUtils.newInstance(keyClass,conf);
      ByteArrayDataInput dataInput =  ByteStreams.newDataInput(bios);
      key.readFields(dataInput);
      kvsname = dataInput.readUTF();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String getKvsname() {
    return kvsname;
  }

  public void setKvsname(String kvsname) {
    this.kvsname = kvsname;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public void setKeyClass(Class<K> keyClass) {
    this.keyClass = keyClass;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }
}
