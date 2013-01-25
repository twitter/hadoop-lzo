package com.hadoop.compression.lzo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.hadoop.mapreduce.LzoIndexOutputFormat;
import com.hadoop.mapreduce.LzoSplitInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedLzoIndexer extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(DistributedLzoIndexer.class);
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
      } else if (path.toString().endsWith(LZO_EXTENSION)) {
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
      System.err.println("No input paths found - perhaps all " +
        ".lzo files have already been indexed.");
      return 0;
    }

    Job job = new Job(getConf());
    job.setJobName("Distributed Lzo Indexer " + Arrays.toString(args));

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

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new DistributedLzoIndexer(), args);
    System.exit(exitCode);
  }

  public static void printUsage() {
    System.err.println("Usage: hadoop jar /path/to/this/jar com.hadoop.compression.lzo.DistributedLzoIndexer <file.lzo | directory> [file2.lzo directory3 ...]");
  }
}
