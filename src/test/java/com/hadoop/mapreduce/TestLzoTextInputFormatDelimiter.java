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
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.GPLNativeCodeLoader;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.util.CompatibilityUtil;

/**
 * Test the LzoTextInputFormat, make sure it splits the file properly and
 * returns the right data.
 */
public class TestLzoTextInputFormatDelimiter extends TestCase {

	public void testNothing() {
    }
    /*
  private static final Log LOG = LogFactory.getLog(TestLzoTextInputFormatDelimiter.class
      .getName());

  private Path outputDir;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Path testBuildData = new Path(System.getProperty("test.build.data", "data"));
    outputDir = new Path(testBuildData, "outputDir");
  }

  private void writeToFile(String contents, Job job, TaskAttemptContext attemptContext) throws IOException, InterruptedException {
	    // prep an LZO file with data

	    TextOutputFormat.setCompressOutput(job, true);
	    TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	    TextOutputFormat.setOutputPath(job, outputDir);

	    TextOutputFormat<Text, Text> output = new TextOutputFormat<Text, Text>();
	    OutputCommitter committer = output.getOutputCommitter(attemptContext);
	    committer.setupJob(job);
	    RecordWriter<Text, Text> rw = null;

	    try {
	      rw = output.getRecordWriter(attemptContext);
	      rw.write(new Text(contents), new Text());
	    } finally {
	      if (rw != null) {
	        rw.close(attemptContext);
	        committer.commitTask(attemptContext);
	        committer.commitJob(job);
	      }
	    }
  }

	private List<String> readRecordsFromFile(Job job, TaskAttemptContext attemptContext) throws IOException, InterruptedException {
	    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
	    TextInputFormat.setInputPaths(job, outputDir);

	    List<String> values = Lists.newArrayList();

	    List<InputSplit> is = inputFormat.getSplits(job);
	    for (InputSplit inputSplit : is) {
	        RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
	            inputSplit, attemptContext);
	        rr.initialize(inputSplit, attemptContext);

	        while (rr.nextKeyValue()) {
	          Text value = rr.getCurrentValue();

	          values.add(value.toString());
	        }

	        rr.close();
	      }
	    return values;
	}

	private List<String> readRecordsFromFileWithDelimiter(String delimiter, Job job, TaskAttemptContext attemptContext) throws IOException, InterruptedException {
		job.getConfiguration().set("textinputformat.record.delimiter", delimiter);

	    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
	    TextInputFormat.setInputPaths(job, outputDir);

	    List<String> values = Lists.newArrayList();

	    List<InputSplit> is = inputFormat.getSplits(job);
	    for (InputSplit inputSplit : is) {
	        RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
	            inputSplit, attemptContext);
	        rr.initialize(inputSplit, attemptContext);

	        while (rr.nextKeyValue()) {
	          Text value = rr.getCurrentValue();

	          values.add(value.toString());
	        }

	        rr.close();
	      }
	    return values;
	}

	final private static char[] hexArray = "0123456789ABCDEF".toCharArray();
	private static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}

	public void testCustomRecordDelimiter() throws IOException, InterruptedException {
		if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
			LOG.warn("Cannot run this test without the native lzo libraries");
			return;
		}

		Configuration conf = new Configuration();
		conf.set("io.compression.codecs", LzopCodec.class.getName());

		Job job = new Job(conf);

	    FileSystem localFs = FileSystem.getLocal(new Configuration());
	    localFs.delete(outputDir, true);
	    localFs.mkdirs(outputDir);
	    localFs.close();

	    TaskAttemptContext attemptContext =
	            CompatibilityUtil.newTaskAttemptContext(job.getConfiguration(),
	              new TaskAttemptID(TaskID.forName("task_123_0001_r_000001"), 2));


	    // 12 records with default delimiter of \r or \n or \r\n and just three with custom \n
	    String data = "I have eaten\rthe plums\rthat were in\rthe icebox\r\n" +
	    		"and which\ryou were probably\rsaving\rfor breakfast\r\n" +
	    		"Forgive me\rthey were delicious\rso sweet\rand so cold";
		writeToFile(data, job, attemptContext);

	    // read it with default delimiter
		List<String> defaultRecords = readRecordsFromFile(job, attemptContext);
		assertEquals(12, defaultRecords.size());
//		System.out.println(defaultRecords);
		List<String> expectedDefaultRecords = Lists.newArrayList(
				"I have eaten", "the plums", "that were in", "the icebox",
	    		"and which", "you were probably", "saving", "for breakfast",
	    		"Forgive me", "they were delicious", "so sweet", "and so cold\t");
//		System.out.println(bytesToHex(defaultRecords.get(0).getBytes()));
//		System.out.println(bytesToHex(expectedDefaultRecords.get(0).getBytes()));
//		System.out.println("-");
//		System.out.println(bytesToHex(defaultRecords.get(defaultRecords.size()-1).getBytes()));
//		System.out.println(bytesToHex(expectedDefaultRecords.get(expectedDefaultRecords.size()-1).getBytes()));
		for(int i=0; i<expectedDefaultRecords.size(); i++) {
			assertEquals(expectedDefaultRecords.get(i), defaultRecords.get(i));
		}
		assertEquals(expectedDefaultRecords, defaultRecords);

		// read it with custom delimiter
		List<String> customRecords = readRecordsFromFileWithDelimiter("\n", job, attemptContext);
		assertEquals(3, customRecords.size());
		List<String> expectedCustomRecords = Lists.newArrayList(
				"I have eaten\rthe plums\rthat were in\rthe icebox\n",
	    		"and which\ryou were probably\rsaving\rfor breakfast",
	    		"Forgive me\rthey were delicious\rso sweet\rand so cold");
		for(int i=0; i<expectedCustomRecords.size(); i++) {
			assertEquals(bytesToHex(expectedCustomRecords.get(i).getBytes()), bytesToHex(customRecords.get(i).getBytes()));
		}
		assertEquals(expectedCustomRecords, customRecords);
	}
    */
}
