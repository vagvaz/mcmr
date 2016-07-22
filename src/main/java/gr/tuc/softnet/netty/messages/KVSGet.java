package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * Created by vagvaz on 1/04/16.
 */
public class KVSGet<K extends WritableComparable> extends MCMessage {
  public static final String TYPE = "KVSGet";
  String name;
  Class<K> keyClass;
  K key;

  public  KVSGet(){
    super(TYPE);
  }

  public KVSGet(String name,K key, Class<K> keyClass){
    super(TYPE);
    this.key = key;
    this.keyClass = keyClass;
    this.name = name;
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
      output.writeUTF(name);
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
      key = ReflectionUtils.newInstance(keyClass,null);
      ByteArrayDataInput dataInput =  ByteStreams.newDataInput(bios);
      key.readFields(dataInput);
      name = dataInput.readUTF();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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
