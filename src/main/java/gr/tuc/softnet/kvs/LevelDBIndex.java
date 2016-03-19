package gr.tuc.softnet.kvs;


import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.PrintUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.iq80.leveldb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by vagvaz on 8/17/15.
 */
public class LevelDBIndex<K extends WritableComparable,V extends Writable> implements KeyValueStore<K,V> {
  private static final String JNI_DB_FACTORY_CLASS_NAME = "org.fusesource.leveldbjni.JniDBFactory";
  private static final String JAVA_DB_FACTORY_CLASS_NAME = "org.iq80.leveldb.impl.Iq80DBFactory";
  private WriteOptions writeOptions;
  private DB keysDB;
  private DB dataDB;
  private File baseDirFile;
  private File keydbFile;
  private File datadbFile;
  private Options options;
  private LevelDBIterator<K> keyIterator;
  private LevelDBDataIterator<K,V> valuesIterator;
  private int batchSize = 50000;
  private int batchCount = 0;
  private WriteBatch batch;
  private DBFactory dbfactory;
  private Logger log = LoggerFactory.getLogger(LevelDBIndex.class);
  private boolean isclosed = false;
  private KVSConfiguration configuraiton;
  private Class<V> valueClass;
  private Class<K> keyClass;
  private Configuration hadoopConf = new Configuration();

  public LevelDBIndex(KVSConfiguration configuration){
    this.configuraiton = configuration;
    baseDirFile = new File(configuration.getBaseDir());
    if (baseDirFile.exists() && baseDirFile.isDirectory()) {
      for (File f : baseDirFile.listFiles()) {
        f.delete();
      }
      baseDirFile.delete();
    } else if (baseDirFile.exists()) {
      baseDirFile.delete();
    }
    baseDirFile.mkdirs();
    keydbFile = new File(baseDirFile.toString() + "/" + configuration.getName()+".keydb");
    datadbFile = new File(baseDirFile.toString() + "/" + configuration.getName()+".datadb");
    options = new Options();
    options.writeBufferSize(50 * 1024 * 1024);
    options.createIfMissing(true);
    //        options.blockSize(LQPConfiguration.getInstance().conf()
    //            .getInt("leads.processor.infinispan.leveldb.blocksize", 16)*1024*1024);
    //        options.cacheSize(LQPConfiguration.getInstance().conf()
    //            .getInt("leads.processor.infinispan.leveldb.cachesize", 256)*1024*1024);
    options.blockSize(4 * 1024);

    options.compressionType(CompressionType.SNAPPY);
    options.cacheSize(64 * 1024 * 1024);
    dbfactory = factory;
    //        JniDBFactory.pushMemoryPool(128*1024*1024);
    try {
      keysDB = dbfactory.open(keydbFile, options.verifyChecksums(true));
      dataDB = dbfactory.open(datadbFile, options);
      writeOptions = new WriteOptions();
      writeOptions.sync(false);

      batch = dataDB.createWriteBatch();
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      keyClass = (Class<K>) this.getClass().getClassLoader().loadClass(configuration.getKeyClass());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    valueClass = null;
    try {
      valueClass = (Class<V>) this.getClass().getClassLoader().loadClass(configuration.getValueClass());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }


  }

  public void printKeys() {
    DBIterator iterator = keysDB.iterator();
    iterator.seekToFirst();
    while (iterator.hasNext()) {
      System.out.println(asString(iterator.next().getKey()));
    }

    iterator = dataDB.iterator();
    iterator.seekToFirst();
    while (iterator.hasNext()) {
      System.out.println(asString(iterator.next().getKey()));
    }
    System.out.println("values-------------\n");
  }

  @Override public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
    keyIterator = new LevelDBIterator(keysDB,keyClass);
    return keyIterator;
  }

  @Override public Iterator<V> getKeyIterator(K key, Integer counter) {
    //        if(valuesIterator != null){
    //            valuesIterator.close();
    //        }
    if (valuesIterator == null)
      valuesIterator = new LevelDBDataIterator(dataDB, key, counter,valueClass);
    //        }

    valuesIterator.initialize(key, counter);
    return valuesIterator;

  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return null;
  }
  @Override
  public boolean contains(K key) {
    return get(key) == null;
  }

  public static void main(String[] args) {
    for (int i = 0; i < 1; i++) {
      KVSConfiguration configuration = new KVSConfiguration("testlevelb");
      configuration.setLocal(true);
      configuration.setBaseDir("/tmp/testdb");
      configuration.setMaterialized(true);
      configuration.setKeyClass(Text.class.getCanonicalName());
      configuration.setValueClass(Text.class.getCanonicalName());
      LevelDBIndex<Text,Text> index = new LevelDBIndex(configuration);
      Text t = null;
      if (t == null)
        t = initTuple();
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
      //               index.printKeys();

      start = System.nanoTime();
      //        for(int key = 0; key < numberofkeys; key++) {
      int totalcounter = 0;
      for (Map.Entry<Text, Integer> entry : index.getKeysIterator()) {
        counter = 0;
        //            System.out.println("iter key "+entry.getKey());
        Iterator<Text> iterator = index.getKeyIterator(entry.getKey(), entry.getValue());
        while (iterator.hasNext()) {
          try {
            Text tt =  iterator.next();
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

  //    80.156.73.113:11222;80.156.73.116:11222;80.156.73.123:11222;80.156.73.128:11222
  //    ;
  @Override public synchronized void flush() {
    try {
      if (dataDB != null) {
        if (batch != null) {
          dataDB.write(batch);
          batch.close();
          batch = dataDB.createWriteBatch();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    //        printKeys();

  }

  private static ArrayList<Text> generate(int numberofkeys, int numberofvalues) {
    String baseKey = "baseKeyString";
    System.out.println("generate");
    ArrayList<Text> result = new ArrayList<>(numberofkeys * numberofvalues);
    for (int key = 0; key < numberofkeys; key++) {
      //            System.out.println("key " + key);
      for (int value = 0; value < numberofvalues; value++) {
        result.add(new Text(baseKey + key));
      }
    }
    Collections.shuffle(result);
    return result;
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
      f +=("akasjd;flkasjd;flkjas;dlfkjas;ldkfja;lskdjf;laskjdf;laskjdfl;aksjdflkjh;goheriolugtleikrgtiw2u3h4rpo243ur430" +
              "opurt038u4ptr80i3jmtfpojk;flkms;fdk;a'sldkfasjdf;alsdjf;alsdkjf;laskjdf;lajdlfjkal;sdkjf;laskdjfladshujro2" +
              "uy349o2u34ol2j34l;k2jm3;4lk2j3oi42u3o492uy3948729384ou2o34u923874928374928374928upoaisdjf;lkamfdl.amdfljkaor8u290347928736409263492734092874928374928" +
              "akasjd;flkasjd;flkjas;dlfkjas;ldkfja;lskdjf;laskjdf;laskjdfl;aksjdflkjh;goheriolugtleikrgtiw2u3h4rpo243ur430" +
              "opurt038u4ptr80i3jmtfpojk;flkms;fdk;a'sldkfasjdf;alsdjf;alsdkjf;laskjdf;lajdlfjkal;sdkjf;laskdjfladshujro2" +
              "uy349o2u34ol2j34l;k2jm3;4lk2j3oi42u3o492uy3948729384ou2o34u923874928374928374928upoaisdjf;lkamfdl.amdfljkaor8u290347928736409263492734092874928374928" +
              "uy349o2u34ol2j34l;k2jm3;4lk2j3oi42u3o492uy3948729384ou2o34u923874928374928374928upoaisdjf;lkamfdl.amdfljkaor8u290347928736409263492734092874928374928" +
              "sadfasdfajdkflasjdf;lakjdf;lkajsdfl;kajsdlf;kjas;ldkfjal;sdjkfla;skdjflaskdjf;lakjdflaksjdf;lkajsdflkas"+
              "sadfasdfajdkflasjdf;lakjdf;lkajsdfl;kajsdlf;kjas;ldkfjal;sdjkfla;skdjflaskdjf;lakjdflaksjdf;lkajsdflkas");
    }
    t.set(f);
    return t;
  }


  private static Text getTuple(int key, int value) {

    //        t = new Tuple();
    //        int key = 4;
    //        int value = 5;
    //        t.setAttribute("key","baseKey"+Integer.toString(key));
    //        t.setAttribute("value",Integer.toString(value));
    //        for(int i = 0 ; i < 4; i++){
    //            t.setAttribute("key-"+key+"-"+i,key);
    //            t.setAttribute("value-"+value+"-"+i,value);
    //            t.setAttribute("keyvalue-"+key+"."+value+"-"+i,key*value);
    //        }
    Text t = new Text();
    t.set("ajdkflasjdf;lakjdf;lkajsdfl;kajsdlf;kjas;ldkfjal;sdjkfla;skdjflaskdjf;lakjdflaksjdf;lkajsdflkas");
    return t;
  }

  @Override public synchronized void put(K key, V value) {
    try {


      ByteArrayDataOutput keyBytes = ByteStreams.newDataOutput();
      ByteArrayDataOutput valueBytes = ByteStreams.newDataOutput();
      ByteArrayDataOutput keyWrapper = ByteStreams.newDataOutput();
      key.write(keyBytes);
      value.write(valueBytes);
      byte[] count = keysDB.get(keyBytes.toByteArray());
      Integer counter = -1;
      if (count == null) {
        counter = 0;
      } else {
        //            ByteBuffer bytebuf = ByteBuffer.wrap(count);
        counter = Integer.parseInt(new String(count));
        counter += 1;
      }
      KeyWrapper<K> wrapper = new KeyWrapper<K>(key,counter);
      wrapper.write(keyWrapper);
      byte[] keyvalue = bytes(counter.toString());
      keysDB.put(keyBytes.toByteArray(), keyvalue, writeOptions);
      //        encoder = new BasicBSONEncoder();
      //        System.out.println(b.length);
      //        dataDB.put(bytes(key+"{}"+counter),b,writeOptions);
      batch.put(keyWrapper.toByteArray(), valueBytes.toByteArray());
      batchCount++;
      if (batchCount >= batchSize) {
        try {
          dataDB.write(batch, writeOptions);
          batch.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch = dataDB.createWriteBatch();
        batchCount = 0;
      }

    } catch (Exception e) {
      e.printStackTrace();
      PrintUtilities.logStackTrace(log, e.getStackTrace());
    }
  }

  public void add(K keyObject, V valueObject) {
    put(keyObject,valueObject);
  }
  public int size() {
    return 0;
  }

  //

  @Override public synchronized void close() {

    if (isclosed)
      return;
    isclosed = true;
    if (keyIterator != null) {
      keyIterator.close();
    }
    keyIterator = null;
    if (valuesIterator != null) {
      valuesIterator.close();

    }
    valuesIterator = null;
    if (keysDB != null) {
      try {
        keysDB.close();
        dbfactory.destroy(keydbFile, new Options());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    keysDB = null;
    if (dataDB != null) {
      try {
        if (batch != null) {
          batch.close();
        }

        dataDB.close();
        dbfactory.destroy(datadbFile, new Options());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    dataDB = null;
    //        for(File f : keydbFile.listFiles())
    //        {
    //            f.delete();
    //        }
    //        keydbFile.delete();
    //
    //        for(File f : datadbFile.listFiles())
    //        {
    //            f.delete();
    //        }
    //        datadbFile.delete();
    //
    //        baseDirFile = new File(baseDirFile.toString()+"/");
    //        for(File f : baseDirFile.listFiles())
    //        {
    //            f.delete();
    //        }
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

  @Override
  public String getName() {
    return configuraiton.getName();
  }

  @Override public V get(K key) {
    this.flush();
    KeyWrapper<K> wrapper = new KeyWrapper<K>(key,0);
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    try {
      wrapper.write(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
    byte[] values = dataDB.get(output.toByteArray());
    if(values == null){
      return null;
    }
    else{
      ByteArrayDataInput input = ByteStreams.newDataInput(values);
      V result = ReflectionUtils.newInstance(valueClass,hadoopConf);
      try {
        result.readFields(input);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return result;
    }
  }

  @Override public void finalize() {
    System.err.println("Finalize leveldb Index " + this.baseDirFile.toString());
  }
}
