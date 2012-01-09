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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoInputFormatCommon;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * An {@link InputFormat} for lzop compressed text files. Files are broken into
 * lines. Either linefeed or carriage-return are used to signal end of line.
 * Keys are the position in the file, and values are the line of text.
 * <p>
 * See {@link LzoInputFormatCommon} for a description of the boolean property
 * <code>lzo.text.input.format.ignore.nonlzo</code> and how it affects the
 * behavior of this input format.
 */
public class LzoTextInputFormat extends TextInputFormat {
  private final Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> files = super.listStatus(job);

    Configuration conf = job.getConfiguration();
    boolean ignoreNonLzo = LzoInputFormatCommon.getIgnoreNonLzoProperty(conf);

    for (Iterator<FileStatus> iterator = files.iterator(); iterator.hasNext();) {
      FileStatus fileStatus = iterator.next();
      Path file = fileStatus.getPath();
      FileSystem fs = file.getFileSystem(conf);

      if (!LzoInputFormatCommon.isLzoFile(file.toString())) {
        // Get rid of non-LZO files, unless the conf explicitly tells us to
        // keep them.
        // However, always skip over files that end with ".lzo.index", since
        // they are not part of the input.
        if (ignoreNonLzo || LzoInputFormatCommon.isLzoIndexFile(file.toString())) {
          iterator.remove();
        }
      } else {
        //read the index file
        LzoIndex index = LzoIndex.readIndex(fs, file);
        indexes.put(file, index);
      }
    }

    return files;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    if (LzoInputFormatCommon.isLzoFile(filename.toString())) {
      LzoIndex index = indexes.get(filename);
      return !index.isEmpty();
    } else {
      // Delegate non-LZO files to the TextInputFormat base class.
      return super.isSplitable(context, filename);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    Configuration conf = job.getConfiguration();
    // find new start/ends of the filesplit that aligns
    // with the lzo blocks

    List<InputSplit> result = new ArrayList<InputSplit>();

    for (InputSplit genericSplit : splits) {
      FileSplit fileSplit = (FileSplit) genericSplit;
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);

      if (!LzoInputFormatCommon.isLzoFile(file.toString())) {
        // non-LZO file, keep the input split as is.
        result.add(fileSplit);
        continue;
      }

      // LZO file, try to split if the .index file was found
      LzoIndex index = indexes.get(file);
      if (index == null) {
        throw new IOException("Index not found for " + file);
      }

      if (index.isEmpty()) {
        // empty index, keep as is
        result.add(fileSplit);
        continue;
      }

      long start = fileSplit.getStart();
      long end = start + fileSplit.getLength();

      long lzoStart = index.alignSliceStartToIndex(start, end);
      long lzoEnd = index.alignSliceEndToIndex(end, fs.getFileStatus(file).getLen());

      if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
        result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
      }
    }

    return result;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) {
    FileSplit fileSplit = (FileSplit) split;
    if (LzoInputFormatCommon.isLzoFile(fileSplit.getPath().toString())) {
      return new LzoLineRecordReader();
    } else {
      // Delegate non-LZO files to the TextInputFormat base class.
      return super.createRecordReader(split, taskAttempt);
    }
  }
}
