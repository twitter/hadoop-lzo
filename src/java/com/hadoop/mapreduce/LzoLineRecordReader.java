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

package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Reads line from an lzo compressed text file. Treats keys as offset in file
 * and value as line.
 */
public class LzoLineRecordReader extends RecordReader<LongWritable, Text> {

  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private FSDataInputStream fileIn;

  private final LongWritable key = new LongWritable();
  private final Text value = new Text();

  /**
   * Get the progress within the split.
   */
  @Override
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) genericSplit;
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    Configuration job = context.getConfiguration();

    FileSystem fs = file.getFileSystem(job);
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec == null) {
      throw new IOException("Codec for file " + file + " not found, cannot run");
    }

    // open the file and seek to the start of the split
    fileIn = fs.open(split.getPath());

    // creates input stream and also reads the file header
    in = new LineReader(codec.createInputStream(fileIn), job);

    if (start != 0) {
      fileIn.seek(start);

      // read and ignore the first line
      in.readLine(new Text());
      start = fileIn.getPos();
    }

    this.pos = start;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    //since the lzop codec reads everything in lzo blocks
    //we can't stop if the pos == end
    //instead we wait for the next block to be read in when
    //pos will be > end
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
}

