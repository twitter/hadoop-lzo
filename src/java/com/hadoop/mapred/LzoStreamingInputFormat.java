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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.hadoop.compression.lzo.LzoInputFormatCommon;

/**
 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style
 * which is deprecated but still required in places.  Streaming, for example,
 * does a check that the given input format is a descendant of
 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
 * from the new API fails.  In order for streaming to work, you must use
 * com.hadoop.mapred.LzoStreamingInputFormat.
 *
 * See {@link LzoInputFormatCommon} for a description of the boolean property
 * <code>lzo.text.input.format.ignore.nonlzo</code> and how it affects the
 * behavior of this input format.
*/

@SuppressWarnings("deprecation")
public class LzoStreamingInputFormat extends FileInputFormat<Text, Text>
  implements JobConfigurable {

  // Wrapper around DeprecatedLzoTextInputFormat that exposes a couple
  // protected methods so we can delegate to them.
  private class WrappedDeprecatedLzoTextInputFormat extends DeprecatedLzoTextInputFormat {
    public boolean isSplitableWrapper(FileSystem fs, Path filename) {
      return isSplitable(fs, filename);
    }

    public FileStatus[] listStatusWrapper(JobConf conf) throws IOException {
      return listStatus(conf);
    }
  }

  // This class delegates most calls to either DeprecatedLzoTextInputFormat
  // (listStatus, getSplits, isSplitable) or KeyValueTextInputFormat
  // (getRecordReader for non-LZO files).
  private final WrappedDeprecatedLzoTextInputFormat lzoTextInputFormat =
      new WrappedDeprecatedLzoTextInputFormat();
  private final KeyValueTextInputFormat kvTextInputFormat =
      new KeyValueTextInputFormat();

  @Override
  public void configure(JobConf conf) {
    lzoTextInputFormat.configure(conf);
    kvTextInputFormat.configure(conf);
  }

  @Override
  protected FileStatus[] listStatus(JobConf conf) throws IOException {
    // Delegate to DeprecatedLzoTextInputFormat
    return lzoTextInputFormat.listStatusWrapper(conf);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    // Delegate to DeprecatedLzoTextInputFormat
    return lzoTextInputFormat.getSplits(conf, numSplits);
  }

  public RecordReader<Text, Text> getRecordReader(InputSplit split,
    JobConf conf, Reporter reporter) throws IOException {

    FileSplit fileSplit = (FileSplit) split;
    if (LzoInputFormatCommon.isLzoFile(fileSplit.getPath().toString())) {
      reporter.setStatus(split.toString());
      return new LzoStreamingLineRecordReader(conf, (FileSplit)split);
    } else {
      // delegate non-LZO files to KeyValueTextInputFormat
      return kvTextInputFormat.getRecordReader(split, conf, reporter);
    }
  }
}