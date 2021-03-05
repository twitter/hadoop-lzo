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

package com.hadoop.compression.lzo;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Represents the lzo index.
 */
public class LzoIndex {
  public static final String LZO_INDEX_SUFFIX = ".index";
  public static final String LZO_TMP_INDEX_SUFFIX = ".index.tmp";
  public static final long NOT_FOUND = -1;

  private long[] blockPositions_;

   private static ArrayList<Class<? extends LzoIndexSerde>> serdeClasses =
       new ArrayList<Class<? extends LzoIndexSerde>>();

  static {
     serdeClasses.add(LzoBasicIndexSerde.class);
     serdeClasses.add(LzoTinyOffsetsSerde.class);
   }


  /**
   * Create an empty index, typically indicating no index file exists.
   */
  public LzoIndex() { }

  /**
   * Create an index specifying the number of LZO blocks in the file.
   * @param blocks The number of blocks in the LZO file the index is representing.
   */
  public LzoIndex(int blocks) {
    blockPositions_ = new long[blocks];
  }

  /**
   * Set the position for the block.
   *
   * @param blockNumber Block to set pos for.
   * @param pos Position.
   */
  public void set(int blockNumber, long pos) {
    blockPositions_[blockNumber] = pos;
  }

  /**
   * Get the total number of blocks in the index file.
   * @return the total number of blocks
   */
  public int getNumberOfBlocks() {
    return blockPositions_.length;
  }

  /**
   * Get the block offset for a given block.
   * @param block the given block
   * @return the byte offset into the file where this block starts.  It is the developer's
   * responsibility to call getNumberOfBlocks() to know appropriate bounds on the parameter.
   * The argument block should satisfy 0 &le; block &lt; getNumberOfBlocks().
   */
  public long getPosition(int block) {
    return blockPositions_[block];
  }

  /**
   * Find the next lzo block start from the given position.
   *
   * @param pos The position to start looking from.
   * @return Either the start position of the block or -1 if it couldn't be found.
   */
  public long findNextPosition(long pos) {
    int block = Arrays.binarySearch(blockPositions_, pos);

    if (block >= 0) {
      // direct hit on a block start position
      return blockPositions_[block];
    } else {
      block = Math.abs(block) - 1;
      if (block > blockPositions_.length - 1) {
        return NOT_FOUND;
      }
      return blockPositions_[block];
    }
  }

  /**
   * Return true if the index has no blocks set.
   *
   * @return true if the index has no blocks set.
   */
  public boolean isEmpty() {
    return blockPositions_ == null || blockPositions_.length == 0;
  }

  /**
   * Nudge a given file slice start to the nearest LZO block start no earlier than
   * the current slice start.
   *
   * @param start The current slice start
   * @param end The current slice end
   * @return The smallest block offset in the index between [start, end), or
   *         NOT_FOUND if there is none such.
   */
  public long alignSliceStartToIndex(long start, long end) {
    if (start != 0) {
      // find the next block position from
      // the start of the split
      long newStart = findNextPosition(start);
      if (newStart == NOT_FOUND || newStart >= end) {
        return NOT_FOUND;
      }
      start = newStart;
    }
    return start;
  }

  /**
   * Nudge a given file slice end to the nearest LZO block end no earlier than
   * the current slice end.
   *
   * @param end The current slice end
   * @param fileSize The size of the file, i.e. the max end position.
   * @return The smallest block offset in the index between [end, fileSize].
   */
  public long alignSliceEndToIndex(long end, long fileSize) {
    long newEnd = findNextPosition(end);
    if (newEnd != NOT_FOUND) {
      end = newEnd;
    } else {
      // didn't find the next position
      // we have hit the end of the file
      end = fileSize;
    }
    return end;
  }

  /**
   * Read the index of the lzo file.

   * @param fs The index file is on this file system.
   * @param lzoFile the file whose index we are reading -- NOT the index file itself.  That is,
   * pass in filename.lzo, not filename.lzo.index, for this parameter.
   * @throws IOException if file not found
   * @return LzoIndex 
   */
  public static LzoIndex readIndex(FileSystem fs, Path lzoFile) throws IOException {
    FSDataInputStream indexIn = null;
    Path indexFile = lzoFile.suffix(LZO_INDEX_SUFFIX);

    try {
      indexIn = fs.open(indexFile);
    } catch (IOException fileNotFound) {
      // return empty index, fall back to the unsplittable mode
      return new LzoIndex();
    }

    long firstLong = indexIn.readLong();
    LzoIndexSerde serde = null;
    for (Class<? extends LzoIndexSerde> candidateClass : serdeClasses) {
      LzoIndexSerde candidate = null;
      candidate = quietGetInstance(candidateClass);
      if (candidate.accepts(firstLong)) {
        serde = candidate;
        break;
       }
     }
    serde.prepareToRead(indexIn);
    // Sized for at least 1 256MB HDFS block with 256KB Lzo blocks.
    // if it's less than that, you shouldn't bother indexing anyway.
    ArrayList<Long> offsets = new ArrayList<Long>(1024);
    while (serde.hasNext()) {
      offsets.add(serde.next());
    }
    serde.finishReading();
    LzoIndex index = new LzoIndex(offsets.size());
    for (int i = 0; i < offsets.size(); i++) {
      index.set(i, offsets.get(i));
    }
    return index;
  }

  /**
   * Index an lzo file to allow the input format to split them into separate map
   * jobs.
   *
   * @param fs File system that contains the file.
   * @param lzoFile the lzo file to index.  For filename.lzo, the created index file will be
   * filename.lzo.index.
   * @throws IOException if could not find codec
   */
  public static void createIndex(FileSystem fs, Path lzoFile)
  throws IOException {

    Configuration conf = fs.getConf();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(lzoFile);
    if (null == codec) {
      throw new IOException("Could not find codec for file " + lzoFile +
        " - you may need to add the LZO codec to your io.compression.codecs " +
        "configuration in core-site.xml");
    }
    ((Configurable) codec).setConf(conf);

    FSDataInputStream is = null;
    FSDataOutputStream os = null;
    LzoIndexSerde writer = new LzoTinyOffsetsSerde();
    Path outputFile = lzoFile.suffix(LZO_INDEX_SUFFIX);
    Path tmpOutputFile = lzoFile.suffix(LZO_TMP_INDEX_SUFFIX);

    // Track whether an exception was thrown or not, so we know to either
    // delete the tmp index file on failure, or rename it to the new index file on success.
    boolean indexingSucceeded = false;
    try {
      is = fs.open(lzoFile);
      os = fs.create(tmpOutputFile);
      writer.prepareToWrite(os);
      LzopDecompressor decompressor = (LzopDecompressor) codec.createDecompressor();
      // Solely for reading the header
      codec.createInputStream(is, decompressor);
      int numCompressedChecksums = decompressor.getCompressedChecksumsCount();
      int numDecompressedChecksums = decompressor.getDecompressedChecksumsCount();

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

        // See LzopInputStream.getCompressedData
        boolean isUncompressedBlock = (uncompressedBlockSize == compressedBlockSize);
        int numChecksumsToSkip = isUncompressedBlock ?
            numDecompressedChecksums : numDecompressedChecksums + numCompressedChecksums;
        long pos = is.getPos();
        // write the pos of the block start
        writer.writeOffset(pos - 8);
        // seek to the start of the next block, skip any checksums
        is.seek(pos + compressedBlockSize + (4 * numChecksumsToSkip));
      }
      // If we're here, indexing was successful.
      indexingSucceeded = true;
    } finally {
      // Close any open streams.
      if (is != null) {
        is.close();
      }

      if (os != null) {
        os.close();
      }

      if (!indexingSucceeded) {
        // If indexing didn't succeed (i.e. an exception was thrown), clean up after ourselves.
        fs.delete(tmpOutputFile, false);
      } else {
        // Otherwise, rename filename.lzo.index.tmp to filename.lzo.index.
        fs.rename(tmpOutputFile, outputFile);
      }
    }
  }

  private static LzoIndexSerde quietGetInstance(Class<? extends LzoIndexSerde> klass) throws IOException {
    LzoIndexSerde instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
    return instance;
   }
}

