import com.google.inject.Guice;
import com.google.inject.Injector;
import gr.tuc.softnet.core.*;
import gr.tuc.softnet.engine.MCJobProxy;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSProxy;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.mapred.examples.DataLoader;
import gr.tuc.softnet.mapred.examples.LineDataLoader;
import gr.tuc.softnet.mapred.examples.wordcount.WordCountSubmitter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import rx.exceptions.Exceptions;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Created by vagvaz on 26/05/16.
 */
public class WordCountDriver {
  public static void main(String[] args) throws InterruptedException {
    Injector injector = Guice.createInjector(new MCNodeModule());
    InjectorUtils.setInjector(injector);
    NodeManager nodemanager = injector.getInstance(NodeManager.class);
    if(args.length > 0 ) {
      nodemanager.initialize(args[0]);
    }else{
      nodemanager.initialize(StringConstants.DEFAULT_CONF_DIR);
    }
    Vector<File> files = new Vector<>();
    files.add(new File("/home/vagvaz/error"));
    DataLoader<IntWritable,Text> dataLoader = new LineDataLoader("documents",nodemanager, injector, files);
    Thread thread = new Thread(dataLoader);
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Exceptions.throwIfFatal(new Exception());
    KeyValueStore store = InjectorUtils.getInjector().getInstance(KVSManager.class).getKVS("documents");
    Iterator<Map.Entry> it = store.iterator().iterator();
    while(it.hasNext()){
      Map.Entry entry = it.next();
      System.out.println(entry.getKey() + "     " + entry.getValue());
    }

      MCJobProxy jobProxy = WordCountSubmitter.submit(false, false, false, "documents", "words",
        nodemanager.getMicrocloudInfo().keySet());

      jobProxy.waitForCompletion();
      KVSManager kvsManager = InjectorUtils.getInjector().getInstance(KVSManager.class);
      Iterable<Map.Entry<WritableComparable, Writable>> iterator = kvsManager.getKVS("words").iterator();
      for (Map.Entry<WritableComparable, Writable> entry : iterator) {
        System.out.println(entry.getKey() + " v: " + entry.getValue());
      }
//      kvsManager.destroyKVS("words");
      System.out.println("Bye Bye");

  }
}
