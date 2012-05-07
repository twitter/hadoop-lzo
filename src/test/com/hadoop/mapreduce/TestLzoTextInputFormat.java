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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.compression.lzo.GPLNativeCodeLoader;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoInputFormatCommon;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestLzoTextInputFormat extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestLzoTextInputFormat.class
      .getName());

  private MessageDigest md5;
  private final String lzoFileName = "part-r-00001" + new LzopCodec().getDefaultExtension();
  private Path outputDir;
  
  //test both bigger outputs and small one chunk ones
  private static final int OUTPUT_BIG = 10485760;
  private static final int OUTPUT_SMALL = 50000;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    md5 = MessageDigest.getInstance("MD5");
    Path testBuildData = new Path(System.getProperty("test.build.data", "data"));
    outputDir = new Path(testBuildData, "outputDir");
  }

  /**
   * Make sure the lzo index class works as described.
   */
  public void testLzoIndex() {
    LzoIndex index = new LzoIndex();
    assertTrue(index.isEmpty());
    index = new LzoIndex(4);
    index.set(0, 0);
    index.set(1, 5);
    index.set(2, 10);
    index.set(3, 15);
    assertFalse(index.isEmpty());

    assertEquals(0, index.findNextPosition(-1));
    assertEquals(5, index.findNextPosition(1));
    assertEquals(5, index.findNextPosition(5));
    assertEquals(15, index.findNextPosition(11));
    assertEquals(15, index.findNextPosition(15));
    assertEquals(-1, index.findNextPosition(16));
    
    assertEquals(5, index.alignSliceStartToIndex(3, 20));
    assertEquals(15, index.alignSliceStartToIndex(15, 20));
    assertEquals(10, index.alignSliceEndToIndex(8, 30));
    assertEquals(10, index.alignSliceEndToIndex(10, 30));
    assertEquals(30, index.alignSliceEndToIndex(17, 30));
    assertEquals(LzoIndex.NOT_FOUND, index.alignSliceStartToIndex(16, 20));
  }

  /**
   * Index the file and make sure it splits properly.
   * 
   * @throws NoSuchAlgorithmException
   * @throws IOException
   * @throws InterruptedException
   */
  public void testWithIndex() throws NoSuchAlgorithmException, IOException,
      InterruptedException {
    
    runTest(true, OUTPUT_BIG);
    runTest(true, OUTPUT_SMALL);
  }

  /**
   * Don't index the file and make sure it can be processed anyway.
   * 
   * @throws NoSuchAlgorithmException
   * @throws IOException
   * @throws InterruptedException
   */
  public void testWithoutIndex() throws NoSuchAlgorithmException, IOException,
      InterruptedException {
    
    runTest(false, OUTPUT_BIG);
    runTest(false, OUTPUT_SMALL);
  }

  /**
   * Generate random data, compress it, index and md5 hash the data.
   * Then read it all back and md5 that too, to verify that it all went ok.
   * 
   * @param testWithIndex Should we index or not?
   * @param charsToOutput How many characters of random data should we output.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws InterruptedException
   */
  private void runTest(boolean testWithIndex, int charsToOutput) throws IOException,
      NoSuchAlgorithmException, InterruptedException {

    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    Configuration conf = new Configuration();
    conf.setLong("fs.local.block.size", charsToOutput / 2);
    // reducing block size to force a split of the tiny file
    conf.set("io.compression.codecs", LzopCodec.class.getName());
    
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(outputDir, true);
    localFs.mkdirs(outputDir);

    Job job = new Job(conf);
    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
    TextOutputFormat.setOutputPath(job, outputDir);

    TaskAttemptContext attemptContext = new TaskAttemptContext(job.getConfiguration(),
        new TaskAttemptID("123", 0, false, 1, 2));

    // create some input data
    byte[] expectedMd5 = createTestInput(outputDir, localFs, attemptContext, charsToOutput);
   
    if (testWithIndex) {
      Path lzoFile = new Path(outputDir, lzoFileName);
      LzoIndex.createIndex(localFs, lzoFile);
    }

    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    TextInputFormat.setInputPaths(job, outputDir);
    
    List<InputSplit> is = inputFormat.getSplits(job);
    //verify we have the right number of lzo chunks
    if (testWithIndex && OUTPUT_BIG == charsToOutput) {
      assertEquals(3, is.size());
    } else {
      assertEquals(1, is.size());
    }

    // let's read it all and calculate the md5 hash
    for (InputSplit inputSplit : is) {
      RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
          inputSplit, attemptContext);
      rr.initialize(inputSplit, attemptContext);

      while (rr.nextKeyValue()) {
        Text value = rr.getCurrentValue();

        md5.update(value.getBytes(), 0, value.getLength());
      }

      rr.close();
    }

    localFs.close();
    assertTrue(Arrays.equals(expectedMd5, md5.digest()));
  }

  /**
   * Creates an lzo file with random data.
   * 
   * @param outputDir Output directory.
   * @param fs File system we're using.
   * @param attemptContext Task attempt context, contains task id etc. 
   * @throws IOException
   * @throws InterruptedException
   */
  private byte[] createTestInput(Path outputDir, FileSystem fs, TaskAttemptContext attemptContext, 
      int charsToOutput) throws IOException, InterruptedException {

    TextOutputFormat<Text, Text> output = new TextOutputFormat<Text, Text>();
    RecordWriter<Text, Text> rw = null;

    md5.reset();
   
    try {
      rw = output.getRecordWriter(attemptContext);

      char[] chars = "abcdefghijklmnopqrstuvwxyz\u00E5\u00E4\u00F6"
          .toCharArray();

      Random r = new Random(System.currentTimeMillis());
      Text key = new Text();
      Text value = new Text();
      int charsMax = chars.length - 1;
      for (int i = 0; i < charsToOutput;) {
        i += fillText(chars, r, charsMax, key);
        i += fillText(chars, r, charsMax, value);
        rw.write(key, value);
        md5.update(key.getBytes(), 0, key.getLength());
        // text output format writes tab between the key and value
        md5.update("\t".getBytes("UTF-8"));
        md5.update(value.getBytes(), 0, value.getLength());
      }
    } finally {
      if (rw != null) {
        rw.close(attemptContext);
        OutputCommitter committer = output.getOutputCommitter(attemptContext);
        committer.commitTask(attemptContext);
        committer.cleanupJob(attemptContext);
      }
    }

    byte[] result = md5.digest();
    md5.reset();
    return result;
  }

  private int fillText(char[] chars, Random r, int charsMax, Text text) {
    StringBuilder sb = new StringBuilder();
    // get a reasonable string length
    int stringLength = r.nextInt(charsMax * 2);
    for (int j = 0; j < stringLength; j++) {
      sb.append(chars[r.nextInt(charsMax)]);
    }
    text.set(sb.toString());
    return stringLength;
  }

  public void testIgnoreNonLzoTrue()
      throws IOException, InterruptedException, NoSuchAlgorithmException {
    runTestIgnoreNonLzo(true, OUTPUT_BIG, true);
    runTestIgnoreNonLzo(true, OUTPUT_SMALL, true);
    runTestIgnoreNonLzo(false, OUTPUT_BIG, true);
    runTestIgnoreNonLzo(false, OUTPUT_SMALL, true);
  }

  public void testIgnoreNonLzoFalse()
      throws IOException, InterruptedException, NoSuchAlgorithmException {
    runTestIgnoreNonLzo(true, OUTPUT_BIG, false);
    runTestIgnoreNonLzo(true, OUTPUT_SMALL, false);
    runTestIgnoreNonLzo(false, OUTPUT_BIG, false);
    runTestIgnoreNonLzo(false, OUTPUT_SMALL, false);
  }

  private void runTestIgnoreNonLzo(boolean testWithIndex, int charsToOutput,
    boolean ignoreNonLzo) throws IOException, InterruptedException, NoSuchAlgorithmException {
    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    Configuration conf = new Configuration();
    conf.setLong("fs.local.block.size", charsToOutput / 2);
    // reducing block size to force a split of the tiny file
    conf.set("io.compression.codecs", LzopCodec.class.getName());
    conf.setBoolean(LzoInputFormatCommon.IGNORE_NONLZO_KEY, ignoreNonLzo);

    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(outputDir, true);
    localFs.mkdirs(outputDir);

    // Create a non-LZO input file and put it alongside the LZO files.
    Path nonLzoFile = new Path(outputDir, "part-r-00001");
    localFs.createNewFile(nonLzoFile);
    FSDataOutputStream outputStream = localFs.create(nonLzoFile);
    outputStream.writeBytes("key1\tvalue1\nkey2\tvalue2\nkey3\tvalue3\n");
    outputStream.close();

    Job job = new Job(conf);
    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
    TextOutputFormat.setOutputPath(job, outputDir);

    TaskAttemptContext attemptContext = new TaskAttemptContext(job.getConfiguration(),
        new TaskAttemptID("123", 0, false, 1, 2));

    // create some input data
    byte[] expectedMd5 = createTestInput(outputDir, localFs, attemptContext, charsToOutput);

    if (testWithIndex) {
      Path lzoFile = new Path(outputDir, lzoFileName);
      LzoIndex.createIndex(localFs, lzoFile);
    }

    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    TextInputFormat.setInputPaths(job, outputDir);

    // verify we have the right number of input splits
    List<InputSplit> is = inputFormat.getSplits(job);
    int numExpectedLzoSplits = 0;
    int numExpectedNonLzoSplits = 0;
    int numActualLzoSplits = 0;
    int numActualNonLzoSplits = 0;
    if (!ignoreNonLzo) {
      numExpectedNonLzoSplits += 1;
    }
    if (testWithIndex && OUTPUT_BIG == charsToOutput) {
      numExpectedLzoSplits += 3;
    } else {
      numExpectedLzoSplits += 1;
    }
    assertEquals(numExpectedLzoSplits + numExpectedNonLzoSplits, is.size());

    // Verify that we have the right number of each kind of split and the right
    // data inside the splits.
    List<String> expectedNonLzoLines = new ArrayList<String>();
    if (!ignoreNonLzo) {
      expectedNonLzoLines.add("key1\tvalue1");
      expectedNonLzoLines.add("key2\tvalue2");
      expectedNonLzoLines.add("key3\tvalue3");
    }
    List<String> actualNonLzoLines = new ArrayList<String>();
    for (InputSplit inputSplit : is) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      Path file = fileSplit.getPath();
      RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
          inputSplit, attemptContext);
      rr.initialize(inputSplit, attemptContext);
      if (LzoInputFormatCommon.isLzoFile(file.toString())) {
        numActualLzoSplits += 1;

        while (rr.nextKeyValue()) {
          Text value = rr.getCurrentValue();

          md5.update(value.getBytes(), 0, value.getLength());
        }

        rr.close();
      } else {
        numActualNonLzoSplits += 1;

        while (rr.nextKeyValue()) {
          actualNonLzoLines.add(rr.getCurrentValue().toString());
        }
      }
    }
    localFs.close();
    assertEquals(numExpectedLzoSplits, numActualLzoSplits);
    assertEquals(numExpectedNonLzoSplits, numActualNonLzoSplits);
    assertTrue(Arrays.equals(expectedMd5, md5.digest()));
    assertEquals(expectedNonLzoLines, actualNonLzoLines);
  }
}
