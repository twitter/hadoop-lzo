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

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Test the LzoOutputFormat, make sure that it can write files of different sizes and read them back in
 * identically.
 */
public class TestLzopOutputStream extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestLzopOutputStream.class);

  private String inputDataPath;

  // Filenames of various sizes to read in and verify.
  private final String bigFile = "100000.txt";
  private final String mediumFile = "1000.txt";
  private final String smallFile = "100.txt";
  private final String issue20File = "issue20-lzop.txt";
  private FileSystem localFs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    inputDataPath = System.getProperty("test.build.data", "data");
    Configuration conf = new Configuration();
    conf.set("io.compression.codecs", LzopCodec.class.getName());
    localFs = FileSystem.getLocal(conf).getRaw();
  }

  /**
   * Test against a 100,000 line file with multiple LZO blocks.
   */
  public void testBigFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(bigFile);
  }

  /**
   * Test against a 1,000 line file with a single LZO block.
   */
  public void testMediumFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(mediumFile);
  }

  /**
   * Test against a 100 line file that is a single LZO block.  Moreover, this
   * file compresses to a size larger than its uncompressed size, so the LZO 
   * format mandates that it's stored differently on disk.  Instead of the usual block
   * format, which is 
   * uncompressed size | compressed size | uncompressed checksum | compressed checksum | compressed data
   * in this case the block gets stored as
   * uncompressed size | uncompressed size | uncompressed checksum | uncompressed data
   * with no additional checksum.  Thus the read has to follow a slightly different codepath.
   */
  public void testSmallFile() throws NoSuchAlgorithmException, IOException,
  InterruptedException {    
    runTest(smallFile);
  }

  /**
   * The LZO specification says that we should write the uncompressed bytes
   * rather than the compressed bytes if the compressed buffer is actually
   * larger ('&gt;') than the uncompressed buffer.
   *
   * To conform to the standard, this means we have to write the uncompressed
   * bytes also when they have exactly the same size as the compressed bytes.
   * (the '==' in '&lt;=').
   *
   * The input data of this test is known to compress to the same size as the
   * uncompressed data.  Hence we verify that we handle the boundary condition
   * correctly.
   *
   */
  public void testIssue20File() throws NoSuchAlgorithmException, IOException,
  InterruptedException {
    runTest(issue20File);
  }

  /**
   * Test that reading an lzo-compressed file produces the same lines as reading the equivalent
   * flat file.  The test opens both the compressed and flat file, successively reading each
   * line by line and comparing.
   */
  private void runTest(String filename) throws IOException,
  NoSuchAlgorithmException, InterruptedException {

    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      LOG.warn("Cannot run this test without the native lzo libraries");
      return;
    }

    // Assumes the flat file is at filename, and the compressed version is filename.lzo
    File textFile = new File(inputDataPath, filename);
    File lzoFile = new File(inputDataPath, filename + new LzopCodec().getDefaultExtension());
    File lzoOutFile = new File(inputDataPath, "output_" + filename + new LzopCodec().getDefaultExtension());
    File lzoIndexFile = new File(lzoOutFile.getAbsolutePath() + LzoIndex.LZO_INDEX_SUFFIX);
    if (lzoOutFile.exists()) {
      lzoOutFile.delete();
    }
    LOG.info("Comparing files " + textFile + " and " + lzoFile);

    //
    // First, read in the text file, and write each line to an lzop output stream.
    //
    
    // Set up the text file reader.
    BufferedReader textBr = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
    // Set up the LZO writer..
    int lzoBufferSize = 256 * 1024;
    LzoCompressor.CompressionStrategy strategy = LzoCompressor.CompressionStrategy.LZO1X_1;
    LzoCompressor lzoCompressor = new LzoCompressor(strategy, lzoBufferSize);
    LzopOutputStream lzoOut = new LzopOutputStream(new FileOutputStream(lzoOutFile),
        new DataOutputStream(new FileOutputStream(lzoIndexFile)),
        lzoCompressor, lzoBufferSize, strategy);

    // Now read line by line and stream out..
    String textLine;
    while ((textLine = textBr.readLine()) != null) {
      textLine += "\n";
      byte[] bytes = textLine.getBytes();
      lzoOut.write(bytes, 0, bytes.length);
    }
    textBr.close();
    lzoOut.close();
    
    //
    // Now, read in the lzo we just wrote, decompressing and verifying line by line with the text file.
    //
    
    // Set up the text file reader.
    BufferedReader textBr2 = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
    // Set up the LZO reader.
    LzopDecompressor lzoDecompressor = new LzopDecompressor(lzoBufferSize);
    LzopInputStream lzoIn = new LzopInputStream(new FileInputStream(lzoOutFile.getAbsolutePath()), lzoDecompressor, lzoBufferSize);
    BufferedReader lzoBr = new BufferedReader(new InputStreamReader(lzoIn));

    // Now read line by line and compare.
    String textLine2;
    String lzoLine;
    int line = 0;
    while ((textLine2 = textBr2.readLine()) != null) {
      line++;
      lzoLine = lzoBr.readLine();
      if (!lzoLine.equals(textLine2)) {
        LOG.error("LZO decoding mismatch on line " + line + " of file " + filename);
        LOG.error("Text line: [" + textLine2 + "], which has length " + textLine2.length());
        LOG.error("LZO line: [" + lzoLine + "], which has length " + lzoLine.length());
      }
      assertEquals(lzoLine, textLine2);
    }
    // Verify that the lzo file is also exhausted at this point.
    assertNull(lzoBr.readLine());
    
    lzoBr.close();
    textBr2.close();

    // verify the index file:

    Path lzoOutPath = new Path(lzoOutFile.getAbsolutePath());
    LzoIndex lzoIndex = LzoIndex.readIndex(localFs, lzoOutPath);

    // create offline index to compare.
    assertTrue(lzoIndexFile.delete());
    LzoIndex.createIndex(localFs, lzoOutPath);
    LzoIndex expectedIndex = LzoIndex.readIndex(localFs, lzoOutPath);

    assertEquals(lzoIndex.getNumberOfBlocks(), expectedIndex.getNumberOfBlocks());
    for (int i=0; i<lzoIndex.getNumberOfBlocks(); i++) {
      assertEquals(lzoIndex.getPosition(i), expectedIndex.getPosition(i));
    }
  }
}
