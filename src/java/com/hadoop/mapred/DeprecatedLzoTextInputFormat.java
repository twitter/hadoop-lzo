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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoInputFormatCommon;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style 
 * which is deprecated but still required in places.  Streaming, for example, 
 * does a check that the given input format is a descendant of 
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.  In order for streaming to work, you must use
 * com.hadoop.mapred.DeprecatedLzoTextInputFormat, not 
 * com.hadoop.mapreduce.LzoTextInputFormat.  The classes attempt to be alike in
 * every other respect.
 * <p>
 * Note that to use this input format properly with hadoop-streaming, you should
 * also set the property <code>stream.map.input.ignoreKey=true</code>. That will
 * replicate the behavior of the default TextInputFormat by stripping off the byte
 * offset keys from the input lines that get piped to the mapper process.
 * <p>
 * See {@link LzoInputFormatCommon} for a description of the boolean property
 * <code>lzo.text.input.format.ignore.nonlzo</code> and how it affects the
 * behavior of this input format.
*/

@SuppressWarnings("deprecation")
public class DeprecatedLzoTextInputFormat extends TextInputFormat {
  private final Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

  @Override
  protected FileStatus[] listStatus(JobConf conf) throws IOException {
    List<FileStatus> files = new ArrayList<FileStatus>(Arrays.asList(super.listStatus(conf)));

    boolean ignoreNonLzo = LzoInputFormatCommon.getIgnoreNonLzoProperty(conf);

    Iterator<FileStatus> it = files.iterator();
    while (it.hasNext()) {
      FileStatus fileStatus = it.next();
      Path file = fileStatus.getPath();

      if (!LzoInputFormatCommon.isLzoFile(file.toString())) {
        // Get rid of non-LZO files, unless the conf explicitly tells us to
        // keep them.
        // However, always skip over files that end with ".lzo.index", since
        // they are not part of the input.
        if (ignoreNonLzo || LzoInputFormatCommon.isLzoIndexFile(file.toString())) {
          it.remove();
        }
      } else {
        FileSystem fs = file.getFileSystem(conf);
        LzoIndex index = LzoIndex.readIndex(fs, file);
        indexes.put(file, index);
      }
    }

    return files.toArray(new FileStatus[] {});
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    if (LzoInputFormatCommon.isLzoFile(filename.toString())) {
      LzoIndex index = indexes.get(filename);
      return !index.isEmpty();
    } else {
      // Delegate non-LZO files to the TextInputFormat base class.
      return super.isSplitable(fs, filename);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    FileSplit[] splits = (FileSplit[])super.getSplits(conf, numSplits);
    // Find new starts/ends of the filesplit that align with the LZO blocks.

    List<FileSplit> result = new ArrayList<FileSplit>();

    for (FileSplit fileSplit: splits) {
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
        // Empty index, keep it as is.
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

    return result.toArray(new FileSplit[result.size()]);
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    if (LzoInputFormatCommon.isLzoFile(fileSplit.getPath().toString())) {
      reporter.setStatus(split.toString());
      return new DeprecatedLzoLineRecordReader(conf, (FileSplit)split);
    } else {
      // delegate non-LZO files to the TextInputFormat base class.
      return super.getRecordReader(split, conf, reporter);
    }
  }
}
