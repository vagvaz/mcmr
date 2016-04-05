package gr.tuc.softnet.kvs;


import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.core.WritableComparableSerializer;
import gr.tuc.softnet.core.WritableSerializer;
import rx.Subscriber;


/**
 * Created by vagvaz on 10/11/15.
 */
public class MapDBIndex<K extends WritableComparable, V extends Writable>
    implements KeyValueStore<K, V> {

  BTreeMap<KeyWrapper<K>, V> dataDB;
  boolean closed = false;
  private KVSConfiguration configuration;
  private DB theDb;
  private
  BTreeMap<K, Integer> keysDB;
  private File baseDirFile;
  private File keydbFile;
  private File datadbFile;
  private Iterable<Map.Entry<K, Integer>> keyIterator;
  private MapDBDataIterator valuesIterator;
  //  private int batchSize = ;
  private int batchCount = 0;

  private org.slf4j.Logger log = LoggerFactory.getLogger(MapDBIndex.class);
  private Class<V> valueClass;
  private Class<K> keyClass;

  public MapDBIndex(KVSConfiguration configuration) {
    this.configuration = configuration;
    baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    baseDirFile.getParentFile().mkdirs();
    keydbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".keydb");
    datadbFile = new File(baseDirFile.toString() + "/" + configuration.getName() + ".datadb");

    keyClass = null;
    try {
      keyClass =
          (Class<K>) configuration
              .getKeyClass();//this.getClass().getClassLoader().loadClass(configuration.getKeyClass());
    } catch (Exception e) {
      e.printStackTrace();
    }
    valueClass = null;
    try {
      valueClass =
          (Class<V>) configuration
              .getValueClass();//this.getClass().getClassLoader().loadClass(configuration.getValueClass());
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      theDb =
          DBMaker.newTempFileDB().compressionEnable().mmapFileEnableIfSupported()
              .transactionDisable().closeOnJvmShutdown().deleteFilesAfterClose().asyncWriteEnable()
              .make();
      KeyWrapper tmpWrapper = new KeyWrapper(keyClass);
      keysDB =
          theDb.createTreeMap(keydbFile.getName())
              .keySerializer(new WritableComparableSerializer<K>(keyClass)).makeOrGet();
      dataDB =
          theDb.createTreeMap(datadbFile.getName()).nodeSize(100)
              .keySerializer(new WritableComparableSerializer<>(tmpWrapper))
              .valueSerializer(new WritableSerializer<V>(valueClass)).makeOrGet();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    for (int i = 0; i < 1; i++) {
      KVSConfiguration configuration = new KVSConfiguration("testmapdb");
      configuration.setLocal(true);
      configuration.setBaseDir("/tmp/testdb");
      configuration.setMaterialized(true);
      configuration.setKeyClass(Text.class);
      configuration.setValueClass(Text.class);
      MapDBIndex<Text, Text> index = new MapDBIndex(configuration);
      Text t = null;
      if (t == null) {
        t = initTuple();
      }
      int numberofkeys = 500000;
      int numberofvalues = 2;
      String baseKey = "baseKeyString";

      long start = System.nanoTime();
      ArrayList<Text> tuples = generate(numberofkeys, numberofvalues);
      System.out.println("insert");
      for (Text k : tuples) {
        //            System.out.println("key " + key);
        //            for(int value =0; value < numberofvalues; value++){
        index.add(k, t);
        //            }
      }
      index.flush();
      long end = System.nanoTime();
      long dur = end - start;
      dur /= 1000000;
      int total = numberofkeys * numberofvalues;
      double avg = total / (double) dur;

      System.out.println("Put " + (total) + " in " + (dur) + " avg " + avg);
      int counter = 0;

//                 index.printKeys();

      start = System.nanoTime();
      //        for(int key = 0; key < numberofkeys; key++) {
      int totalcounter = 0;
      for (Map.Entry<Text, Integer> entry : index.getKeysIterator()) {
        counter = 0;
        //            System.out.println("iter key "+entry.getKey());
        Iterator<Text> iterator = index.getKeyIterator(entry.getKey(), entry.getValue());
        while (iterator.hasNext()) {
          try {
            Text tt = iterator.next();
            //                    String t = (String)iterator.next();
            //                System.out.println(t.getAttribute("key")+" --- " + t.getAttribute("value"));
            counter++;
            totalcounter++;
          } catch (NoSuchElementException e) {
            break;
          }
        }
        //                ((LevelDBDataIterator)iterator).close();
        if (counter != numberofvalues) {
          System.err.println("Iteration failed for key " + entry.getKey() + " c " + counter);
        }
      }
      end = System.nanoTime();
      dur = end - start;
      dur /= 1000000;
      avg = total / (double) dur;
      System.out.println("Iterate " + (totalcounter) + " in " + (dur) + " avg " + avg);
      index.close();
      System.out.println("exit---");
    }
    System.err.println("Bye bye after pressing any button");
    try {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static Text initTuple() {
    Text t = new Text();
    int key = 4;
    int value = 5;
    String f = new String();
    for (int i = 0; i < 4; i++) {
//      t.setAttribute("key-" + key + "-" + i, key);
//      t.setAttribute("value-" + value + "-" + i, value);
//      t.setAttribute("keyvalue-" + key + "." + value + "-" + i, key * value);
      f +=
          ("akasjd;flkasjd;flkjas;dlfkjas;ldkfja;lskdjf;laskjdf;laskjdfl;aksjdflkjh;goheriolugtleikrgtiw2u3h4rpo243ur430"
           +
           "opurt038u4ptr80i3jmtfpojk;flkms;fdk;a'sldkfasjdf;alsdjf;alsdkjf;laskjdf;lajdlfjkal;sdkjf;laskdjfladshujro2"
           +
           "uy349o2u34ol2j34l;k2jm3;4lk2j3oi42u3o492uy3948729384ou2o34u923874928374928374928upoaisdjf;lkamfdl.amdfljkaor8u290347928736409263492734092874928374928"
           +
           "akasjd;flkasjd;flkjas;dlfkjas;ldkfja;lskdjf;laskjdf;laskjdfl;aksjdflkjh;goheriolugtleikrgtiw2u3h4rpo243ur430"
           +
           "opurt038u4ptr80i3jmtfpojk;flkms;fdk;a'sldkfasjdf;alsdjf;alsdkjf;laskjdf;lajdlfjkal;sdkjf;laskdjfladshujro2"
           +
           "uy349o2u34ol2j34l;k2jm3;4lk2j3oi42u3o492uy3948729384ou2o34u923874928374928374928upoaisdjf;lkamfdl.amdfljkaor8u290347928736409263492734092874928374928"
           +
           "sadfasdfajdkflasjdf;lakjdf;lkajsdfl;kajsdlf;kjas;ldkfjal;sdjkfla;skdjflaskdjf;lakjdflaksjdf;lkajsdflkas");
    }
    t.set(f);
    return t;
  }

  private static ArrayList<Text> generate(int numberofkeys, int numberofvalues) {
    String baseKey = "baseKeyString";
    System.out.println("generate");
    ArrayList<Text> result = new ArrayList(numberofkeys * numberofvalues);
    for (int key = 0; key < numberofkeys; key++) {
      //            System.out.println("key " + key);
      for (int value = 0; value < numberofvalues; value++) {
        result.add(new Text(baseKey + key));
      }
    }
    Collections.shuffle(result);
    return result;
  }

  public void printKeys() {
    Iterator iterator = keysDB.descendingMap().entrySet().iterator();
    //    iterator.seekToFirst();
    while (iterator.hasNext()) {
      Map.Entry e = (Map.Entry) iterator.next();
      System.out.println(e.getKey() + " -> " + e.getValue());
    }
    System.out.println("keyvalues++++++++++++++++++\n");
    iterator = dataDB.descendingMap().entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry e = (Map.Entry) iterator.next();
      System.out.println(e.getKey());
    }
    System.out.println("values-------------\n");
  }

  @Override
  public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
    keyIterator = new MapDBKeyIterator(keysDB.descendingMap().entrySet());
    return keyIterator;
  }

  public Iterator<V> getKeyIterator(K key, Integer counter) {
    //        if(valuesIterator != null){
    //            valuesIterator.close();
    //        }
    K realKey = key;// key.substring(0,key.lastIndexOf("{}"));
    if (valuesIterator == null) {
      valuesIterator = new MapDBDataIterator(dataDB, realKey, counter, valueClass);
    }
    //        }

    valuesIterator.initialize(realKey, counter);
    return valuesIterator;

  }

  //    80.156.73.113:11222;80.156.73.116:11222;80.156.73.123:11222;80.156.73.128:11222
  //    ;
  public synchronized void flush() {

  }

  public void put(K key, V value) {

    try {

      Integer counter = keysDB.get((key));

      if (counter == null) {
        counter = 0;
      } else {
        counter += 1;
      }

      keysDB.put(key, counter);
      KeyWrapper wrapper = new KeyWrapper(key, counter);
//           ByteArrayDataOutput keyOutput = ByteStreams.newDataOutput();
//           wrapper.write(keyOutput);

      //        System.out.println(b.length);
      //        dataDB.put(bytes(key+"{}"+counter),b,writeOptions);
      dataDB.put(wrapper, value);

    } catch (Exception e) {
      e.printStackTrace();
      PrintUtilities.logStackTrace(log, e.getStackTrace());
    }
  }

  public void add(K keyObject, V valueObject) {
    put(keyObject, valueObject);
  }

  //

  public V get(K key) {
    KeyWrapper wrapper = new KeyWrapper(key, 0);
    ByteArrayDataOutput keyOutput = ByteStreams.newDataOutput();
    try {
      wrapper.write(keyOutput);
    } catch (IOException e) {
      e.printStackTrace();
    }

    //        System.out.println(b.length);
    //        dataDB.put(bytes(key+"{}"+counter),b,writeOptions);
    return dataDB.get(keyOutput.toByteArray());

  }

  @Override
  public int size() {
//      int sum = 0;
//      for(Integer  i : keysDB.values()){
//          sum += i;
//      }
    return keysDB.size();
  }

  public Iterable<Map.Entry<K, Integer>> keysIterator() {
    return keysIterator();
  }

  public Iterator<V> valuesIterator(K key) {
    return null;
  }

  public Iterable<Map.Entry<K, V>> iterator() {
    return null;
  }


  public boolean contains(K key) {
    return keysDB.containsKey(key);
  }

  public synchronized void close() {
    if (this.closed) {
      return;
    }
    closed = true;
    if (keyIterator != null) {
      //      keyIterator.close();

    }
    keyIterator = null;
    if (valuesIterator != null) {
      //      valuesIterator.close();
    }
    valuesIterator = null;
    //    if(keysDB != null){
    //      try {
    //        keysDB.close();
    //      } catch (Exception e) {
    //        e.printStackTrace();
    //      }
    //    }
    //    keysDB = null;
    //    if(dataDB != null){
    //      try {
    //        dataDB.close();
    //      } catch (Exception e) {
    //        e.printStackTrace();
    //      }
    //    }
    dataDB = null;
    theDb.close();
    if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    //        System.out.println("press");
    //        try {
    //            System.in.read();
    //        } catch (IOException e) {
    //            e.printStackTrace();
    //        }
    //        JniDBFactory.popMemoryPool();
  }

  public String getName() {
    return configuration.getName();
  }


  @Override
  public void finalize() {
    System.err.println("Finalize leveldb Index " + this.baseDirFile.toString());
  }

  @Override
  public void call(Subscriber<? super Map.Entry<K, V>> subscriber) {
    rx.Observable.create(this).subscribe(subscriber);
  }
}
