import com.google.inject.Guice;
import com.google.inject.Injector;
import gr.tuc.softnet.core.*;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.mapred.examples.wordcount.WordCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by vagvaz on 03/04/16.
 */
public class testTaskInstantiation {
    public static void main(String[] args) throws Exception {
        Injector injector = Guice.createInjector(new MCNodeModule());
        InjectorUtils.setInjector(injector);
        NodeManager nodemanager = injector.getInstance(NodeManager.class);
        if(args.length > 0 ) {
            nodemanager.initialize(args[0]);
        }else{
            nodemanager.initialize(StringConstants.DEFAULT_CONF_DIR);
        }
        System.out.println(nodemanager.getID());
        System.out.println(nodemanager.getLocalcloudName());
        KVSManager kvsManager = injector.getInstance(KVSManager.class);
        KVSConfiguration kvsConfiguration = new KVSConfiguration("testInput");
        kvsConfiguration.setCacheType(StringConstants.PIPELINE);
        kvsConfiguration.setMaterialized(false);
        kvsConfiguration.setKeyClass(Text.class);
        kvsConfiguration.setValueClass(Text.class);
        kvsManager.createKVS("testInput",kvsConfiguration);
        KeyValueStore<Text,Text> kvs = kvsManager.getKVS("testInput");
        kvs.put( new Text("afou"),new Text("oso"));
        kvs.put( new Text("afou2"),new Text("oso2"));
        kvs.put( new Text("afou1"),new Text("oso1"));
        TaskManager taskManager = InjectorUtils.getInjector().getInstance(TaskManager.class);
        TaskConfiguration configuration = new TaskConfiguration();
        configuration.setID("mytask");
        configuration.setKeyClass(Text.class);
        configuration.setValueClass(Text.class);
        configuration.setOutKeyClass(Text.class);
        configuration.setOutValueClass(IntWritable.class);
        configuration.setJar("/home/vagvaz/Projects/Idea/multisite-mr/target/multi-site-mr-1.0-SNAPSHOT.jar");
        configuration.setMap(true);
        configuration.setMapOuputKeyClass(Text.class);
        configuration.setMapOutputValueClass(IntWritable.class);
        configuration.setMapperClass(WordCountMapper.class);
        configuration.setInput("testInput");
        configuration.setOutput("testInput");
        taskManager.initialize();
        taskManager.startTask(configuration);

        nodemanager.waitForExit();
    }
}
