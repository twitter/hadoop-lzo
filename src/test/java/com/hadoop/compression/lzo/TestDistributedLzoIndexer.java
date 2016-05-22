package com.hadoop.compression.lzo;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDistributedLzoIndexer extends TestCase {
  public void testConfigureDefaults() {
    Configuration conf = new Configuration();
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);
    assertEquals(DistributedLzoIndexer.LZO_INDEXING_RECURSIVE_DEFAULT, indexer.getLzoRecursiveIndexing());
    assertEquals(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_DEFAULT, indexer.getLzoSkipIndexingSmallFiles());
    assertEquals(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_DEFAULT, indexer.getLzoSmallFileSize());
  }

  public void testConfigureSettings() {
    Configuration conf = new Configuration();
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_RECURSIVE_KEY, false);
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_KEY, true);
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, 5 * 1024L);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);
    assertEquals(false, indexer.getLzoRecursiveIndexing());
    assertEquals(true, indexer.getLzoSkipIndexingSmallFiles());
    assertEquals(5 * 1024L, indexer.getLzoSmallFileSize());
  }

  protected void doTestIsSmallFile(long fileSize, long smallThreshold, boolean expectedResult) {
    Configuration conf = new Configuration();
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, smallThreshold);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);
    FileStatus status = new FileStatus(fileSize, false, 3, 512L, 100L, new Path("/tmp/my/file"));

    assertEquals(expectedResult, indexer.isSmallFile(status));
  }

  public void testIsSmallFileSmaller() throws Exception {
    doTestIsSmallFile(500L, 1000L, true);
  }

  public void testIsSmallFileEquals() throws Exception {
    doTestIsSmallFile(500L, 500L, true);
  }

  public void testIsSmallFileGreater() throws Exception {
    doTestIsSmallFile(500L, 200L, false);
  }

  public void testShouldIndexFileNotLzoFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);

    File tempFile = File.createTempFile("TestDistributedLzoIndexer", ".foo");
    FileStatus status = new FileStatus(5L, false, 3, 512L, 100L, new Path(tempFile.getAbsolutePath()));

    assertEquals(false, indexer.shouldIndexFile(status, fs));
  }

  public void testShouldIndexFileSkipSmallFiles() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_KEY, true);
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, 100L);
    FileSystem fs = FileSystem.getLocal(conf);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);

    File tempFile = File.createTempFile("TestDistributedLzoIndexer", ".lzo");
    FileStatus status = new FileStatus(50L, false, 3, 512L, 100L, new Path(tempFile.getAbsolutePath()));

    assertEquals(false, indexer.shouldIndexFile(status, fs));
  }

  public void testShouldIndexFileIndexNonexistent() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_KEY, true);
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, 100L);
    FileSystem fs = FileSystem.getLocal(conf);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);

    File tempFile = File.createTempFile("TestDistributedLzoIndexer", ".lzo");
    FileStatus status = new FileStatus(200L, false, 3, 512L, 100L, new Path(tempFile.getAbsolutePath()));

    assertEquals(true, indexer.shouldIndexFile(status, fs));
  }

  public void testShouldIndexFileEmptyIndexExists() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_KEY, true);
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, 100L);
    FileSystem fs = FileSystem.getLocal(conf);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);

    File tempFile = File.createTempFile("TestDistributedLzoIndexer", ".lzo");
    FileStatus status = new FileStatus(200L, false, 3, 512L, 100L, new Path(tempFile.getAbsolutePath()));

    String tempFileIndexPath = tempFile.getAbsolutePath() + LzoIndex.LZO_INDEX_SUFFIX;
    File tempFileIndex = new File(tempFileIndexPath);
    if (!tempFileIndex.createNewFile()) {
      throw new IOException("Could not create temp file for testing " + tempFileIndex);
    }

    assertEquals(true, indexer.shouldIndexFile(status, fs));
  }

  public void testShouldIndexFileIndexExists() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DistributedLzoIndexer.LZO_INDEXING_SKIP_SMALL_FILES_KEY, true);
    conf.setLong(DistributedLzoIndexer.LZO_INDEXING_SMALL_FILE_SIZE_KEY, 100L);
    FileSystem fs = FileSystem.getLocal(conf);
    DistributedLzoIndexer indexer = new DistributedLzoIndexer();
    indexer.configure(conf);

    File tempFile = File.createTempFile("TestDistributedLzoIndexer", ".lzo");
    FileStatus status = new FileStatus(200L, false, 3, 512L, 100L, new Path(tempFile.getAbsolutePath()));

    String tempFileIndexPath = tempFile.getAbsolutePath() + LzoIndex.LZO_INDEX_SUFFIX;
    File tempFileIndex = new File(tempFileIndexPath);

    OutputStream fos = new FileOutputStream(tempFileIndex);
    fos.write(1);
    fos.close();

    assertEquals(false, indexer.shouldIndexFile(status, fs));
  }
}
