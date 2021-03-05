package com.hadoop.compression.lzo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.hadoop.compression.lzo.util.CompatibilityUtil;
import com.hadoop.mapreduce.LzoIndexOutputFormat;
import com.hadoop.mapreduce.LzoSplitInputFormat;
import com.hadoop.mapreduce.LzoSplitRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedLzoIndexer extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(DistributedLzoIndexer.class);
  /**
   * Override the default job name which is generated from the arguments.
   */
  public static final String JOB_NAME_KEY = "lzo.indexer.distributed.job.name";
  /**
   * Override the default length to which the job name will be truncated. Set non-positive to disable.
   */
  public static final String JOB_NAME_MAX_LENGTH_KEY = "lzo.indexer.distributed.job.name.max.length";
  static final String DEFAULT_JOB_NAME_PREFIX = "Distributed Lzo Indexer";
  private static final int DEFAULT_JOB_NAME_MAX_LENGTH = 200;
  private final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();

  private final PathFilter nonTemporaryFilter = new PathFilter() {
    public boolean accept(Path path) {
      return !path.toString().endsWith("/_temporary");
    }
  };

  private void walkPath(Path path, PathFilter pathFilter, List<Path> accumulator) {
    try {
      FileSystem fs = path.getFileSystem(getConf());
      FileStatus fileStatus = fs.getFileStatus(path);

      if (fileStatus.isDir()) {
        FileStatus[] children = fs.listStatus(path, pathFilter);
        for (FileStatus childStatus : children) {
          walkPath(childStatus.getPath(), pathFilter, accumulator);
        }
      } else if (path.toString().endsWith(LZO_EXTENSION) && fileStatus.getLen() > fileStatus.getBlockSize()) {
        Path lzoIndexPath = path.suffix(LzoIndex.LZO_INDEX_SUFFIX);
        if (fs.exists(lzoIndexPath)) {
          // If the index exists and is of nonzero size, we're already done.
          // We re-index a file with a zero-length index, because every file has at least one block.
          if (fs.getFileStatus(lzoIndexPath).getLen() > 0) {
            LOG.info("[SKIP] LZO index file already exists for " + path);
            return;
          } else {
            LOG.info("Adding LZO file " + path + " to indexing list (index file exists but is zero length)");
            accumulator.add(path);
          }
        } else {
          // If no index exists, we need to index the file.
          LOG.info("Adding LZO file " + path + " to indexing list (no index currently exists)");
          accumulator.add(path);
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Error walking path: " + path, ioe);
    }
  }

  static void setJobName(Job job, String[] args) {
    final Configuration conf = job.getConfiguration();

    String name = conf.get(JOB_NAME_KEY, DEFAULT_JOB_NAME_PREFIX + " " + Arrays.toString(args));

    final int maxLength = conf.getInt(JOB_NAME_MAX_LENGTH_KEY, DEFAULT_JOB_NAME_MAX_LENGTH);

    if (maxLength > 0 && name.length() > maxLength) {
      name = name.substring(0, maxLength) + "...";
    }

    job.setJobName(name);
  }

  public int run(String[] args) throws Exception {
    if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
      printUsage();
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    List<Path> inputPaths = new ArrayList<Path>();
    for (String strPath: args) {
      walkPath(new Path(strPath), nonTemporaryFilter, inputPaths);
    }

    if (inputPaths.isEmpty()) {
      LOG.info("No input paths found - perhaps all " +
          ".lzo files have already been indexed.");
      return 0;
    }

    Job job = new Job(getConf());
    setJobName(job, args);

    job.setOutputKeyClass(Path.class);
    job.setOutputValueClass(LongWritable.class);

    // The LzoIndexOutputFormat doesn't currently work with speculative execution.
    // Patches welcome.
    job.getConfiguration().setBoolean(
      "mapred.map.tasks.speculative.execution", false);

    job.setJarByClass(DistributedLzoIndexer.class);
    job.setInputFormatClass(LzoSplitInputFormat.class);
    job.setOutputFormatClass(LzoIndexOutputFormat.class);
    job.setNumReduceTasks(0);
    job.setMapperClass(Mapper.class);

    for (Path p : inputPaths) {
      FileInputFormat.addInputPath(job, p);
    }

    job.submit();

    LOG.info("Started DistributedIndexer " + job.getJobID() + " with " +
        inputPaths.size() + " splits for " + Arrays.toString(args));
    LOG.info("Queue Used: " + job.getConfiguration().get("mapred.job.queue.name"));

    if (job.waitForCompletion(true)) {
      long successfulMappers = CompatibilityUtil.getCounterValue(
          job.getCounters().findCounter(LzoSplitRecordReader.Counters.READ_SUCCESS));

      if (successfulMappers == inputPaths.size()) {
        return 0;
      }

      // some of the mappers failed
      LOG.error("DistributedIndexer " + job.getJobID() + " failed. "
          + (inputPaths.size() - successfulMappers)
          + " out of " + inputPaths.size() + " mappers failed.");
    } else {
      LOG.error("DistributedIndexer job " + job.getJobID() + " failed.");
    }

    return 1; // failure
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new DistributedLzoIndexer(), args);
    System.exit(exitCode);
  }

  public static void printUsage() {
    System.err.println("Usage: hadoop jar /path/to/this/jar com.hadoop.compression.lzo.DistributedLzoIndexer <file.lzo | directory> [file2.lzo directory3 ...]");
  }
}
