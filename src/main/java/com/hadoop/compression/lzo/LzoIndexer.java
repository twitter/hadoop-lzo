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

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LzoIndexer {
  private static final Log LOG = LogFactory.getLog(LzoIndexer.class);

  private final Configuration conf_;
  private final String LZO_EXTENSION = new LzopCodec().getDefaultExtension();
  private final String INDENT_STRING = "  ";
  private final DecimalFormat df_;

  public LzoIndexer(Configuration conf) {
    conf_ = conf;
    df_ = new DecimalFormat("#0.00");
  }

  /**
   * Index the file given by lzoUri in its default filesystem.
   * 
   * @param lzoUri The file to index.
   * @throws IOException
   */
  public void index(Path lzoPath) throws IOException {
    indexInternal(lzoPath, 0);
  }

  /**
   * Return indented space for pretty printing.
   * 
   * @param nestingLevel The indentation level.
   * @return Indented space for the given indentation level.
   */
  private String getNesting(int nestingLevel) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < nestingLevel; i++) {
      sb.append(INDENT_STRING);
    }
    return sb.toString();
  }

  /**
   * Lzo index a given path, calling recursively to index directories when encountered.
   * Files are only indexed if they end in .lzo and have no existing .lzo.index file.
   * 
   * @param lzoPath The base path to index.
   * @param nestingLevel For pretty printing, the nesting level.
   * @throws IOException
   */
  private void indexInternal(Path lzoPath, int nestingLevel) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(lzoPath.toString()), conf_);
    FileStatus fileStatus = fs.getFileStatus(lzoPath);

    // Recursively walk
    if (fileStatus.isDir()) {
      LOG.info(getNesting(nestingLevel) + "LZO Indexing directory " + lzoPath + "...");
      FileStatus[] statuses = fs.listStatus(lzoPath);
      for (FileStatus childStatus: statuses) {
        indexInternal(childStatus.getPath(), nestingLevel + 1);
      }
    } else if (lzoPath.toString().endsWith(LZO_EXTENSION)) {
      Path lzoIndexPath = new Path(lzoPath.toString() + LzoIndex.LZO_INDEX_SUFFIX);
      if (fs.exists(lzoIndexPath)) {
        LOG.info(getNesting(nestingLevel) + "[SKIP] LZO index file already exists for " + lzoPath + "\n");
      } else {
        long startTime = System.currentTimeMillis();
        long fileSize = fileStatus.getLen();

        LOG.info(getNesting(nestingLevel) + "[INDEX] LZO Indexing file " + lzoPath + ", size " + 
                 df_.format(fileSize / (1024.0 * 1024.0 * 1024.0)) + " GB...");
        if (indexSingleFile(fs, lzoPath)) {
          long indexSize = fs.getFileStatus(lzoIndexPath).getLen();
          double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
          LOG.info(getNesting(nestingLevel) + "Completed LZO Indexing in " + df_.format(elapsed) + " seconds (" + 
                   df_.format(fileSize / (1024.0 * 1024.0 * elapsed)) + " MB/s).  Index size is " + 
                   df_.format(indexSize / 1024.0) + " KB.\n");
        }
      }
    }
  }

  /**
   * Create an lzo index for a single file in HDFS.
   * @param fs The filesystem object.
   * @param lzoPath The path to index (must be a file, not a directory).
   * @return
   */
  private boolean indexSingleFile(FileSystem fs, Path lzoPath) {
    try {
      LzoIndex.createIndex(fs, lzoPath);
      return true;
    } catch (IOException e) {
      LOG.error("Error indexing " + lzoPath, e);
      return false;
    }
  }

  /**
   * Run the LzoIndexer on each argument passed via stdin.  The files should be HDFS locations.
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      printUsage();
      System.exit(1);
    }

    LzoIndexer lzoIndexer = new LzoIndexer(new Configuration());
    for (String arg: args) {
      try {
        lzoIndexer.index(new Path(arg));
      } catch (IOException e) {
        LOG.error("Error indexing " + arg, e);
      }
    }
  }

  public static void printUsage() {
    System.out.println("Usage: hadoop jar /path/to/this/jar com.hadoop.compression.lzo.LzoIndexer <file.lzo | directory> [file2.lzo directory3 ...]");
  }
}