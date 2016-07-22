//import com.sun.tools.javadoc.Start;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.LQPConfiguration;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.MCDataBufferImpl;
import gr.tuc.softnet.netty.messages.KVSBatchPut;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import gr.tuc.softnet.netty.messages.StartTask;
import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.*;

/**
 * Created by vagvaz on 22/04/16.
 */
public class MessageTests extends TestCase {


  @Test
  public void testMessageSerialization(){
    TaskConfiguration task = new TaskConfiguration();
    task.setInput("hello");
    task.setMapOuputKeyClass(IntWritable.class);
    task.setMapOutputValueClass(IntWritable.class);
    task.setTargetCloud("foo");
    StartTask t = new StartTask(task);
    byte[] bytes = t.toBytes();
    StartTask tt = new StartTask();
    tt.fromBytes(bytes);
    System.out.println( tt.getConf().getInput() + " " + tt.getConf().getMapOutputKeyClass().toString());
    KVSConfiguration kvsConfiguration = new KVSConfiguration("testInput");
    kvsConfiguration.setCacheType(StringConstants.PIPELINE);
    kvsConfiguration.setMaterialized(false);
    kvsConfiguration.setKeyClass(Text.class);
    kvsConfiguration.setValueClass(Text.class);
    kvsConfiguration.getPartitionerClass();
  }

    @Test
  public void testBatchPut(){
      String name = "foobar";
      KVSConfiguration kvsConf = new KVSConfiguration(name);
      kvsConf.setKeyClass(Text.class);
      kvsConf.setValueClass(IntWritable.class);
      LQPConfiguration configuration = new LQPConfiguration();
      configuration.initialize();
      MCDataBufferImpl buffer = new MCDataBufferImpl(kvsConf,null,name,configuration);

      for(int i = 0; i < 1000; i++){
        Text key  = new Text(Integer.toString(i));
        IntWritable value = new IntWritable(i);
        buffer.append(key,value);
      }
      byte[] bytes = buffer.getBytesAndClear();
      KVSBatchPut put = new KVSBatchPut(Text.class, IntWritable.class, bytes, name);
      MCMessageWrapper wrapper = new MCMessageWrapper(put,1);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(wrapper);
        oos.close();
        byte[] bytes2 = os.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes2,0,bytes2.length);
        ObjectInputStream ois = new ObjectInputStream(bis);
        MCMessageWrapper wrapper2 = (MCMessageWrapper) ois.readObject();
        KVSBatchPut put2 = (KVSBatchPut) wrapper2.getMCMessage();
        assertEquals(put2.getData().length, put.getData().length);

      } catch (IOException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
}
