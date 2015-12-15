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

  private static final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();

  private static final String LZO_INDEXING_SKIP_SMALL_FILES_KEY = "lzo.indexing.skip-small-files.enabled";
  private static final boolean LZO_INDEXING_SKIP_SMALL_FILES_DEFAULT = false;
  private static final String LZO_INDEXING_SMALL_FILE_SIZE_KEY = "lzo.indexing.skip-small-files.size";
  private static final long LZO_INDEXING_SMALL_FILE_SIZE_DEFAULT = 0;
  private static final String LZO_INDEXING_RECURSIVE_KEY = "lzo.indexing.recursive.enabled";
  private static final boolean LZO_INDEXING_RECURSIVE_DEFAULT = true;
  private static final String TEMP_FILE_EXTENSION = "/_temporary";

  private boolean lzoSkipIndexingSmallFiles = LZO_INDEXING_SKIP_SMALL_FILES_DEFAULT;
  private boolean lzoRecursiveIndexing = LZO_INDEXING_RECURSIVE_DEFAULT;
  private long lzoSmallFileSize = LZO_INDEXING_SMALL_FILE_SIZE_DEFAULT;

  private Configuration conf = getConf();

  /**
   * Accepts paths which don't end in TEMP_FILE_EXTENSION
   */
  private final PathFilter nonTemporaryFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.toString().endsWith(TEMP_FILE_EXTENSION);
    }
  };

  /**
   * Returns whether a file should be considered small enough to skip indexing.
   */
  private boolean isSmallFile(FileStatus status) {
    return status.getLen() < lzoSmallFileSize;
  }
  
  private void visitPath(Path path, PathFilter pathFilter, List<Path> accumulator) {
    try {
      FileSystem fs = path.getFileSystem(conf);
      FileStatus fileStatus = fs.getFileStatus(path);

      if (fileStatus.isDirectory()) {
        if (lzoRecursiveIndexing) {
          FileStatus[] children = fs.listStatus(path, pathFilter);
          for (FileStatus childStatus : children) {
            visitPath(childStatus.getPath(), pathFilter, accumulator);
          }
        } else {
          LOG.info("[SKIP] Path " + path + " is a directory and recursion is not enabled.");
        }
      } else if (shouldIndexPath(fileStatus, fs)) {
        accumulator.add(path);
      }
    } catch (IOException ioe) {
      LOG.warn("Error walking path: " + path, ioe);
    }
  }

  private boolean shouldIndexPath(FileStatus fileStatus, FileSystem fs) throws IOException {
    Path path = fileStatus.getPath();
    if (path.toString().endsWith(LZO_EXTENSION)) {
      if (lzoSkipIndexingSmallFiles && isSmallFile(fileStatus)) {
        LOG.info("[SKIP] Skip indexing small files enabled and " + path + " is too small");
        return false;
      }

      Path lzoIndexPath = new Path(path, LzoIndex.LZO_INDEX_SUFFIX);
      if (fs.exists(lzoIndexPath)) {
        // If the index exists and is of nonzero size, we're already done.
        // We re-index a file with a zero-length index, because every file has at least one block.
        if (fileStatus.getLen() > 0) {
          LOG.info("[SKIP] LZO index file already exists for " + path);
          return false;
        } else {
          LOG.info("Adding LZO file " + path + " to indexing list (index file exists but is zero length)");
          return true;
        }
      } else {
        // If no index exists, we need to index the file.
        LOG.info("Adding LZO file " + path + " to indexing list (no index currently exists)");
        return true;
      }
    }
    return false;
  }

  public int run(String[] args) throws Exception {
    if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
      printUsage();
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    lzoSkipIndexingSmallFiles =
        conf.getBoolean(LZO_INDEXING_SKIP_SMALL_FILES_KEY, LZO_INDEXING_SKIP_SMALL_FILES_DEFAULT);

    lzoSmallFileSize =
        conf.getLong(LZO_INDEXING_SMALL_FILE_SIZE_KEY, LZO_INDEXING_SMALL_FILE_SIZE_DEFAULT);

    // Find paths to index based on recursive/not
    lzoRecursiveIndexing =
        conf.getBoolean(LZO_INDEXING_RECURSIVE_KEY, LZO_INDEXING_RECURSIVE_DEFAULT);
    List<Path> inputPaths = new ArrayList<Path>();
    for (String strPath : args) {
      visitPath(new Path(strPath), nonTemporaryFilter, inputPaths);
    }

    if (inputPaths.isEmpty()) {
      LOG.info("No input paths found - perhaps all " +
          ".lzo files have already been indexed.");
      return 0;
    }

    Job job = new Job(conf);
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

    job.submit();

    LOG.info("Started DistributedIndexer " + job.getJobID() + " with " +
        inputPaths.size() + " splits for " + Arrays.toString(args));

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
    int exitCode = ToolRunner.run(new DistributedLzoIndexer(), args);
    System.exit(exitCode);
  }

  public void printUsage() {
    String usage =
        "Command: hadoop jar /path/to/this/jar com.hadoop.compression.lzo.DistributedLzoIndexer <file.lzo | directory> [file2.lzo directory3 ...]" +
        "\nConfiguration options: \"key\" [values] <default> description" +
        "\n" + LZO_INDEXING_SKIP_SMALL_FILES_KEY + " [true,false] <" + LZO_INDEXING_SKIP_SMALL_FILES_DEFAULT + "> When indexing, skip files smaller than " + LZO_INDEXING_SMALL_FILE_SIZE_KEY + " bytes." +
        "\n" + LZO_INDEXING_SMALL_FILE_SIZE_KEY + " [long] <" + LZO_INDEXING_SMALL_FILE_SIZE_DEFAULT + "> When indexing, skip files smaller than this number of bytes if " + LZO_INDEXING_SKIP_SMALL_FILES_KEY + " is true." +
        "\n" + LZO_INDEXING_RECURSIVE_KEY + " [true,false] <" + LZO_INDEXING_RECURSIVE_DEFAULT + "> Look for files to index recursively from paths on command line.";
    System.err.println(usage);
  }
}
