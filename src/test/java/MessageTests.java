import com.sun.tools.javadoc.Start;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.netty.messages.StartTask;
import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

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
  }

}
