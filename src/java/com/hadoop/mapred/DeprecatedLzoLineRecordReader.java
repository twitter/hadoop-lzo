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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

@SuppressWarnings("deprecation")
public class DeprecatedLzoLineRecordReader implements RecordReader<LongWritable, Text> {
  private CompressionCodecFactory codecFactory = null;
  private long start;
  private long pos;
  private final long end;
  private final LineReader in;
  private final FSDataInputStream fileIn;

  DeprecatedLzoLineRecordReader(Configuration conf, FileSplit split) throws IOException {
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(conf);
    codecFactory = new CompressionCodecFactory(conf);
    final CompressionCodec codec = codecFactory.getCodec(file);
    if (codec == null) {
      throw new IOException("No LZO codec found, cannot run.");
    }

    // Open the file and seek to the next split.
    fileIn = fs.open(file);
    // Create input stream and read the file header.
    in = new LineReader(codec.createInputStream(fileIn), conf);
    if (start != 0) {
      fileIn.seek(start);

      // Read and ignore the first line.
      in.readLine(new Text());
      start = fileIn.getPos();
    }

    pos = start;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public boolean next(LongWritable key, Text value) throws IOException {
    // Since the LZOP codec reads everything in LZO blocks, we can't stop if pos == end.
    // Instead, wait for the next block to be read in when pos will be > end.
    while (pos <= end) {
      key.set(pos);

      int newSize = in.readLine(value);
      if (newSize == 0) {
        return false;
      }
      pos = fileIn.getPos();
      return true;
    }
    return false;
  }

  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start)/ (float)(end - start));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}