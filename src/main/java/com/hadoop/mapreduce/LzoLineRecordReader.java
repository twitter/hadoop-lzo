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
 * <https://www.gnu.org/licenses/>.
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

import com.google.common.base.Charsets;
import com.hadoop.compression.lzo.util.CompatibilityUtil;

/**
 * Reads line from an lzo compressed text file. Treats keys as offset in file
 * and value as line.
 */
public class LzoLineRecordReader extends RecordReader<LongWritable, Text> {

	protected static final boolean supportsCustomDelimiters;
	static {
		boolean hasConstructor = false;
		try{
			RecordReader.class.getConstructor(java.io.InputStream.class, org.apache.hadoop.conf.Configuration.class, byte[].class);
			hasConstructor = true;
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		}
		supportsCustomDelimiters = hasConstructor;
	}

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
    Configuration job = CompatibilityUtil.getConfiguration(context);

    FileSystem fs = file.getFileSystem(job);
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (codec == null) {
      throw new IOException("Codec for file " + file + " not found, cannot run");
    }

    // open the file and seek to the start of the split
    fileIn = fs.open(split.getPath());

    // load the custom line delimiter if one is set
    final String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }

    // creates input stream and also reads the file header
    if(supportsCustomDelimiters && recordDelimiterBytes != null) {
      in = new LineReader(codec.createInputStream(fileIn), job, recordDelimiterBytes);
    } else if(!supportsCustomDelimiters && recordDelimiterBytes != null) {
      // TODO: log.warn("Cannot use customDelimiter " + delimiter + " for LZO files because your Hadoop installation doesn't support the LineReader(InputStream, Configuration, byte[]) constructor.  Consider updating to a distribution of Hadoop that includes the patch in HADOOP-7096");
      // https://issues.apache.org/jira/browse/HADOOP-7096
      in = new LineReader(codec.createInputStream(fileIn), job);
    } else { // recordDelimiterBytes == null
      in = new LineReader(codec.createInputStream(fileIn), job);
    }

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

