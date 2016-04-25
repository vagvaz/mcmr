package gr.tuc.softnet.kvs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by vagvaz on 16/02/16.
 */
public interface MCDataBuffer {
    boolean append(WritableComparable key, Writable value);
    void flush();
    void clear();
    byte[] toBytes();
    byte[] getBytesAndClear();
}
