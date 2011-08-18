/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * This class treats a line in the input as a key/value pair separated by a
 * separator character. The separator can be specified in config file
 * under the attribute name key.value.separator.in.input.line. The default
 * separator is the tab character ('\t').
 *
 * Note: this class is basically a copy of
 * {@link org.apache.hadoop.mapred.KeyValueLineRecordReader}, except that it
 * uses {@link DeprecatedLzoLineRecordReader} as the internal line reader.
 */
@SuppressWarnings("deprecation")
public class LzoStreamingLineRecordReader implements RecordReader<Text, Text> {
  private final DeprecatedLzoLineRecordReader lzoLineRecordReader;
  private byte separator = (byte) '\t';
  private LongWritable dummyKey;
  private Text innerValue;

  public Class getKeyClass() {
    return Text.class;
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  public LzoStreamingLineRecordReader(Configuration job, FileSplit split)
    throws IOException {

    lzoLineRecordReader = new DeprecatedLzoLineRecordReader(job, split);
    dummyKey = lzoLineRecordReader.createKey();
    innerValue = lzoLineRecordReader.createValue();
    String sepStr = job.get("key.value.separator.in.input.line", "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  /**
   * Note: copied from org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader,
   * which is a library class that's missing from the hadoop jar that's in the
   * lib/ directory. Maybe it should be added.
   */
  public static int findSeparator(byte[] utf, int start, int length,
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  /** Read key/value pair in a line. */
  @Override
  public synchronized boolean next(Text key, Text value)
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if (lzoLineRecordReader.next(dummyKey, innerValue)) {
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    int pos = findSeparator(line, 0, lineLen, this.separator);
    LzoStreamingLineRecordReader.setKeyValue(key, value, line, lineLen, pos);
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    return lzoLineRecordReader.getProgress();
  }

  @Override
  public synchronized long getPos() throws IOException {
    return lzoLineRecordReader.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    lzoLineRecordReader.close();
  }

  /**
   * Note: copied from org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader,
   * which is a library class that's missing from the hadoop jar that's in the
   * lib/ directory. Maybe it should be added.
   */
  private static void setKeyValue(Text key, Text value, byte[] line,
      int lineLen, int pos) {
    if (pos == -1) {
      key.set(line, 0, lineLen);
      value.set("");
    } else {
      int keyLen = pos;
      byte[] keyBytes = new byte[keyLen];
      System.arraycopy(line, 0, keyBytes, 0, keyLen);
      int valLen = lineLen - keyLen - 1;
      byte[] valBytes = new byte[valLen];
      System.arraycopy(line, pos + 1, valBytes, 0, valLen);
      key.set(keyBytes);
      value.set(valBytes);
    }
  }
}
