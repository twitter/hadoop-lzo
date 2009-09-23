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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * An {@link InputFormat} for lzop compressed text files. Files are broken into
 * lines. Either linefeed or carriage-return are used to signal end of line.
 * Keys are the position in the file, and values are the line of text.
 */
public class LzoTextInputFormat extends FileInputFormat<LongWritable, Text> {

  private final Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> files = super.listStatus(job);

    FileSystem fs = FileSystem.get(job.getConfiguration());
    String fileExtension = new LzopCodec().getDefaultExtension();

    for (Iterator<FileStatus> iterator = files.iterator(); iterator.hasNext();) {
      FileStatus fileStatus = iterator.next();
      Path file = fileStatus.getPath();

      if (!file.toString().endsWith(fileExtension)) {
        //get rid of non lzo files
        iterator.remove();
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
    LzoIndex index = indexes.get(filename);
    return !index.isEmpty();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    // find new start/ends of the filesplit that aligns
    // with the lzo blocks

    List<InputSplit> result = new ArrayList<InputSplit>();
    FileSystem fs = FileSystem.get(job.getConfiguration());

    for (InputSplit genericSplit : splits) {
      // load the index
      FileSplit fileSplit = (FileSplit) genericSplit;
      Path file = fileSplit.getPath();
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
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {

    return new LzoLineRecordReader();
  }
}
