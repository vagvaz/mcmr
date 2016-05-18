package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ap0n on 28/4/2016.
 */
public class Document implements Writable, Serializable {
  private List<String> words;

  public Document() {
    words = new LinkedList<>();
  }

  public Document(List<String> words) {
    this.words = words;
  }

  public List<String> getWords() {
    return words;
  }

  public void setWords(List<String> words) {
    this.words = words;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(words.size());
    for (String w : words) {
      out.writeUTF(w);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (words == null) {
      words = new LinkedList<>();
    } else {
      words.clear();
    }
    int size = in.readInt();
    while (size-- > 0) {
      words.add(in.readUTF());
    }
  }
}
