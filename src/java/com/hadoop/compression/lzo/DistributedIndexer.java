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
package com.hadoop.compression.lzo;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

/**
 * MapReduce job for indexing LZO files
 */
public class DistributedIndexer extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(DistributedIndexer.class);
  private static final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();
  private static final String TMP_SUFFIX = ".tmp";

  public static class LzoSplitsInputFormat extends FileInputFormat<Path, LongWritable> {
    @Override
    protected boolean isSplitable(JobContext ctx, Path filename) {
      return false;
    }

    @Override
    public RecordReader<Path, LongWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
      return new LzoSplitRecordReader();
    }
  }
  public static class LzoSplitRecordReader extends RecordReader<Path, LongWritable> {
    private final LongWritable curValue = new LongWritable(-1);
    private FSDataInputStream inputStream = null;

    private int numChecksums = -1;
    private long totalFileSize = 0;
    private Path outputPath;

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();

      Path lzoFile = split.getPath();
      assert split.getStart() == 0;
      totalFileSize = split.getLength(); // because this is not splittable!

      LzopCodec codec = new LzopCodec();
      codec.setConf(job);
      LzopDecompressor decompressor = (LzopDecompressor) codec.createDecompressor();

      if (codec == null) {
        throw new IOException("Input file at " + lzoFile + " does not appear " +
                              "to be compressed");
      }
      FileSystem fs = lzoFile.getFileSystem(job);
      inputStream = fs.open(lzoFile);
      // Solely for reading the header
      codec.createInputStream(inputStream, decompressor);

      outputPath = new Path(lzoFile.toString() + LzoIndex.LZO_INDEX_SUFFIX);
      numChecksums = decompressor.getChecksumsCount();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      assert inputStream != null;
      assert numChecksums != -1;

      int uncompressedBlockSize = inputStream.readInt();
      if (uncompressedBlockSize == 0)
        return false;
      if (uncompressedBlockSize < 0) {
        throw new EOFException("Unexpected early EOF at position " +
                               inputStream.getPos());
      }

      int compressedBlockSize = inputStream.readInt();
      if (compressedBlockSize <= 0) {
        throw new EOFException("Could not read compressed block size at " +
                               "position " + inputStream.getPos());
      }

      long pos = inputStream.getPos();
      curValue.set(pos - 8); // pointer to before block header
      inputStream.skip(compressedBlockSize + 4 * numChecksums);
      LOG.info("pos: " + pos);
      return true;
    }

    @Override
    public Path getCurrentKey() {
      return outputPath;
    }

    @Override
    public LongWritable getCurrentValue() {
      return curValue;
    }

    @Override
    public float getProgress() throws IOException {
      if (totalFileSize == 0)
        return 0.0f;
      else
        return inputStream.getPos() / totalFileSize;
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
      inputStream = null;
    }
  }


  public static class LzoIndexOutputFormat
    extends OutputFormat<Path, LongWritable> {
    
    @Override
    public RecordWriter<Path, LongWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
      return new LzoIndexRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {

      return new OutputCommitter() {
        public void abortTask(TaskAttemptContext taskContext) { }
        public void cleanupJob(JobContext jobContext) { }
        public void commitTask(TaskAttemptContext taskContext) { }
        public boolean needsTaskCommit(TaskAttemptContext taskContext) {
          return false;
        }
        public void setupJob(JobContext jobContext) { }
        public void setupTask(TaskAttemptContext taskContext) { }
      };
    }
  }


  public static class LzoIndexRecordWriter
    extends RecordWriter<Path, LongWritable> {

    private FSDataOutputStream outStream;
    private final TaskAttemptContext context;
    private Path outputPath;

    public LzoIndexRecordWriter(TaskAttemptContext context) {
      this.context = context;
    }

    private FSDataOutputStream openOutput(Path path) throws IOException {
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      return fs.create(path, false);
    }

    public void write(Path outputPath, LongWritable offset) throws IOException {
      if (outStream == null) {
        outStream = openOutput(outputPath);
      }
      offset.write(outStream);
    }

    public void close(TaskAttemptContext context) throws IOException {
      if (outStream != null) {
        outStream.close();
      }
    }
  }




  private boolean isBlacklisted(Path p) {
    // Don't index temporary output from MR jobs
    return p.toString().endsWith("/_temporary");
  }

  private void walkPath(Path path, List<Path> accumulator) {
    if (isBlacklisted(path)) {
      return;
    }

    try {
      FileSystem fs = path.getFileSystem(getConf());
      FileStatus stat = fs.getFileStatus(path);

      if (stat.isDir()) {
        FileStatus[] children = fs.listStatus(path);
        for (FileStatus childStatus : children) {
          walkPath(childStatus.getPath(), accumulator);
        }
      } else if (path.toString().endsWith(LZO_EXTENSION)) {
        Path lzoIndexPath = new Path(path.toString() + LzoIndex.LZO_INDEX_SUFFIX);
        if (fs.exists(lzoIndexPath)) {
          LOG.info("[SKIP] LZO index file already exists for " + path);
          return;
        }

        LOG.info("Adding path: " + path);
        accumulator.add(path);
      }
    } catch (IOException ioe) {
      LOG.warn("Error walking path: " + path, ioe);
    }
  }

  public int run(String args[]) throws Exception {
    if (args.length == 0 ||
        (args.length == 1 && "--help".equals(args[0]))) {
      System.err.println("usage: DistributedIndexer <paths to index>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }


    ArrayList<Path> toIndex = new ArrayList<Path>();
    for (String arg : args) {
      Path argPath = new Path(arg);
      walkPath(argPath, toIndex);
    }
    LOG.info("Found " + toIndex.size() + " files to index");
    return runJob(toIndex);
  }

  private int runJob(List<Path> toIndex) throws Exception {
    Job job = new Job(getConf(), "Distributed Index");
    job.setJarByClass(DistributedIndexer.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(LzoSplitsInputFormat.class);
    job.setOutputFormatClass(LzoIndexOutputFormat.class);
    job.setMapperClass(Mapper.class);
    job.setOutputKeyClass(Path.class);
    job.setOutputValueClass(LongWritable.class);


    FileOutputFormat.setOutputPath(job, new Path("lzo-testout"));
    for (Path p : toIndex) {
      FileInputFormat.addInputPath(job, p);
    }
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String args[]) throws Exception {
    System.exit(ToolRunner.run(new Configuration(),
                               new DistributedIndexer(),
                               args));
  }
}

