package com.hadoop.mapreduce;

import java.io.EOFException;
import java.io.IOException;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzopDecompressor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LzoSplitRecordReader extends RecordReader<Path, LongWritable> {
  private static final Log LOG = LogFactory.getLog(LzoSplitRecordReader.class);

  private final int LOG_EVERY_N_BLOCKS = 1000;

  private final LongWritable curValue = new LongWritable(-1);
  private FSDataInputStream rawInputStream;
  private TaskAttemptContext context;

  private int numBlocksRead = 0;
  private int numDecompressedChecksums = -1;
  private int numCompressedChecksums = -1;
  private long totalFileSize = 0;
  private Path lzoFile;

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    context = taskAttemptContext;
    FileSplit fileSplit = (FileSplit)genericSplit;
    lzoFile = fileSplit.getPath();
    // The LzoSplitInputFormat is not splittable, so the split length is the whole file.
    totalFileSize = fileSplit.getLength();

    // Jump through some hoops to create the lzo codec.
    Configuration conf = context.getConfiguration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(lzoFile);
    ((Configurable)codec).setConf(conf);

    LzopDecompressor lzopDecompressor = (LzopDecompressor)codec.createDecompressor();
    FileSystem fs = lzoFile.getFileSystem(conf);
    rawInputStream = fs.open(lzoFile);

    // Creating the LzopInputStream here just reads the lzo header for us, nothing more.
    // We do the rest of our input off of the raw stream is.
    codec.createInputStream(rawInputStream, lzopDecompressor);

    // This must be called AFTER createInputStream is called, because createInputStream
    // is what reads the header, which has the checksum information.  Otherwise getChecksumsCount
    // erroneously returns zero, and all block offsets will be wrong.
    numCompressedChecksums = lzopDecompressor.getCompressedChecksumsCount();
    numDecompressedChecksums = lzopDecompressor.getDecompressedChecksumsCount();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    int uncompressedBlockSize = rawInputStream.readInt();
    if (uncompressedBlockSize == 0) {
      // An uncompressed block size of zero means end of file.
      return false;
    } else if (uncompressedBlockSize < 0) {
      throw new EOFException("Could not read uncompressed block size at position " +
                             rawInputStream.getPos() + " in file " + lzoFile);
    }

    int compressedBlockSize = rawInputStream.readInt();
    if (compressedBlockSize <= 0) {
      throw new EOFException("Could not read compressed block size at position " +
                             rawInputStream.getPos() + " in file " + lzoFile);
    }

    // See LzopInputStream.getCompressedData
    boolean isUncompressedBlock = (uncompressedBlockSize == compressedBlockSize);
    int numChecksumsToSkip = isUncompressedBlock ?
            numDecompressedChecksums : numDecompressedChecksums + numCompressedChecksums;

    // Get the current position.  Since we've read two ints, the current block started 8 bytes ago.
    long pos = rawInputStream.getPos();
    curValue.set(pos - 8);
    // Seek beyond the checksums and beyond the block data to the beginning of the next block.
    rawInputStream.seek(pos + compressedBlockSize + (4 * numChecksumsToSkip));
    ++numBlocksRead;

    // Log some progress every so often.
    if (numBlocksRead % LOG_EVERY_N_BLOCKS == 0) {
      LOG.info("Reading block " + numBlocksRead + " at pos " + pos + " of " + totalFileSize + ". Read is " +
               (100.0 * getProgress()) + "% done. ");
    }

    return true;
  }

  @Override
  public Path getCurrentKey() {
    return lzoFile;
  }

  @Override
  public LongWritable getCurrentValue() {
    return curValue;
  }

  @Override
  public float getProgress() throws IOException {
    if (totalFileSize == 0) {
      return 0.0f;
    } else {
      return (float)rawInputStream.getPos() / totalFileSize;
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing input stream after reading " + numBlocksRead + " blocks from " + lzoFile);
    rawInputStream.close();
  }
}

