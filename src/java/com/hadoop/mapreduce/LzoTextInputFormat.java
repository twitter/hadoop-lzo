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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzopCodec.LzopDecompressor;

/**
 * An {@link InputFormat} for lzop compressed text files. Files are broken into
 * lines. Either linefeed or carriage-return are used to signal end of line.
 * Keys are the position in the file, and values are the line of text.
 */
public class LzoTextInputFormat extends FileInputFormat<LongWritable, Text> {

  public static final String LZO_INDEX_SUFFIX = ".index";
  
  private Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();
  
  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> files = super.listStatus(job);

    FileSystem fs = FileSystem.get(job.getConfiguration());
    String fileExtension = new LzopCodec().getDefaultExtension();

    for (Iterator<FileStatus> iterator = files.iterator(); iterator.hasNext();) {
      FileStatus fileStatus = (FileStatus) iterator.next();
      Path file = fileStatus.getPath();
      
      if (!file.toString().endsWith(fileExtension)) {
        //get rid of non lzo files
        iterator.remove();
      } else {
        //read the index file
        LzoIndex index = readIndex(file, fs);
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

      if (start != 0) {
        // find the next block position from
        // the start of the split
        long newStart = index.findNextPosition(start);
        if (newStart == -1 || newStart >= end) {
          // just skip this since it will be handled by another split
          continue;
        }
        start = newStart;
      }

      long newEnd = index.findNextPosition(end);
      if (newEnd != -1) {
        end = newEnd;
      } else {
        //didn't find the next position
        //we have hit the end of the file
        end = fs.getFileStatus(file).getLen();
      }

      result.add(new FileSplit(file, start, end - start, fileSplit
          .getLocations()));
    }

    return result;
  }

  /**
   * Read the index of the lzo file.
   * 
   * @param split
   *          Read the index of this file.
   * @param fs
   *          The index file is on this file system.
   * @throws IOException
   */
  private LzoIndex readIndex(Path file, FileSystem fs) throws IOException {
    FSDataInputStream indexIn = null;
    try {
      Path indexFile = new Path(file.toString() + LZO_INDEX_SUFFIX);
      if (!fs.exists(indexFile)) {
        // return empty index, fall back to the unsplittable mode
        return new LzoIndex();
      }

      long indexLen = fs.getFileStatus(indexFile).getLen();
      int blocks = (int) (indexLen / 8);
      LzoIndex index = new LzoIndex(blocks);
      indexIn = fs.open(indexFile);
      for (int i = 0; i < blocks; i++) {
        index.set(i, indexIn.readLong());
      }
      return index;
    } finally {
      if (indexIn != null) {
        indexIn.close();
      }
    }
  }

  /**
   * Index an lzo file to allow the input format to split them into separate map
   * jobs.
   * 
   * @param fs
   *          File system that contains the file.
   * @param lzoFile
   *          the lzo file to index.
   * @throws IOException
   */
  public static void createIndex(FileSystem fs, Path lzoFile)
      throws IOException {

    Configuration conf = fs.getConf();
    CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
    CompressionCodec codec = factory.getCodec(lzoFile);
    ((Configurable) codec).setConf(conf);

    InputStream lzoIs = null;
    FSDataOutputStream os = null;
    Path outputFile = new Path(lzoFile.toString()
        + LzoTextInputFormat.LZO_INDEX_SUFFIX);
    Path tmpOutputFile = outputFile.suffix(".tmp");
    
    try {
      FSDataInputStream is = fs.open(lzoFile);
      os = fs.create(tmpOutputFile);
      LzopDecompressor decompressor = (LzopDecompressor) codec
          .createDecompressor();
      // for reading the header
      lzoIs = codec.createInputStream(is, decompressor);

      int numChecksums = decompressor.getChecksumsCount();

      while (true) {
        // read and ignore, we just want to get to the next int
        int uncompressedBlockSize = is.readInt();
        if (uncompressedBlockSize == 0) {
          break;
        } else if (uncompressedBlockSize < 0) {
          throw new EOFException();
        }

        int compressedBlockSize = is.readInt();
        if (compressedBlockSize <= 0) {
          throw new IOException("Could not read compressed block size");
        }

        long pos = is.getPos();
        // write the pos of the block start
        os.writeLong(pos - 8);
        // seek to the start of the next block, skip any checksums
        is.seek(pos + compressedBlockSize + (4 * numChecksums));
      }
    } finally {
      if (lzoIs != null) {
        lzoIs.close();
      }

      if (os != null) {
        os.close();
      }
    }
    
    fs.rename(tmpOutputFile, outputFile);
  }

  /**
   * Represents the lzo index.
   */
  static class LzoIndex {

    private long[] blockPositions;

    LzoIndex() {
    }

    LzoIndex(int blocks) {
      blockPositions = new long[blocks];
    }

    /**
     * Set the position for the block.
     * 
     * @param blockNumber
     *          Block to set pos for.
     * @param pos
     *          Position.
     */
    public void set(int blockNumber, long pos) {
      blockPositions[blockNumber] = pos;
    }

    /**
     * Find the next lzo block start from the given position.
     * 
     * @param pos
     *          The position to start looking from.
     * @return Either the start position of the block or -1 if it couldn't be
     *         found.
     */
    public long findNextPosition(long pos) {
      int block = Arrays.binarySearch(blockPositions, pos);

      if (block >= 0) {
        // direct hit on a block start position
        return blockPositions[block];
      } else {
        block = Math.abs(block) - 1;
        if (block > blockPositions.length - 1) {
          return -1;
        }
        return blockPositions[block];
      }
    }

    public boolean isEmpty() {
      return blockPositions == null || blockPositions.length == 0;
    }

  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext taskAttempt) throws IOException, InterruptedException {

    return new LzoLineRecordReader();
  }
}
