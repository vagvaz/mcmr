package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ap0n on 28/4/2016.
 */
// TODO: Test me
public class Document implements Writable {
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
      writeString(out, w);
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
      words.add(readString(in));
    }
  }

  private void writeString(DataOutput out, String string) throws IOException {
    if (string == null) {
      out.writeInt(0);
    } else {
      out.writeInt(string.getBytes().length);
      out.write(string.getBytes());
    }
  }

  private String readString(DataInput in) throws IOException {
    int bytesCounter = in.readInt();
    if (bytesCounter == 0) {
      return null;
    }
    byte[] stringBytes = new byte[bytesCounter];
    for (int i = 0; i < bytesCounter; i++) {
      stringBytes[i] = in.readByte();
    }
    return new String(stringBytes);
  }
}
