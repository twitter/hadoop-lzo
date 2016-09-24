package com.hadoop.compression.lzo;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class TestDistLzoIndexerJobName extends TestCase {

  public void testDefaultName() throws Exception {
    String[] args = new String[]{
        "hdfs://cluster/user/test/output/file-m-00000.lzo",
    };

    Job job = new Job(new Configuration(false));
    DistributedLzoIndexer.setJobName(job, args);

    String expected = DistributedLzoIndexer.DEFAULT_JOB_NAME_PREFIX + " [hdfs://cluster/user/test/output/file-m-00000.lzo]";

    assertEquals(expected, job.getJobName());
  }

  public void testCustomName() throws Exception {
    String[] args = new String[]{
        "ignored",
    };
    String customName = "-<Custom Job Name>-";

    Configuration conf = new Configuration(false);
    conf.set(DistributedLzoIndexer.JOB_NAME_KEY, customName);
    Job job = new Job(conf);
    DistributedLzoIndexer.setJobName(job, args);

    assertEquals(customName, job.getJobName());
  }

  public void testCustomNameTruncation() throws Exception {
    String[] args = new String[]{
        "ignored",
    };

    Configuration conf = new Configuration(false);
    conf.set(DistributedLzoIndexer.JOB_NAME_KEY, "123456789");
    conf.setInt(DistributedLzoIndexer.JOB_NAME_MAX_LENGTH_KEY, 5);
    Job job = new Job(conf);
    DistributedLzoIndexer.setJobName(job, args);

    assertEquals("12345...", job.getJobName());
  }

  public void testCustomLengthTruncation() throws Exception {
    String[] args = new String[]{
        "hdfs://cluster/user/test/output/file-m-00000.lzo",
        "hdfs://cluster/user/test/output/file-m-00001.lzo",
        "hdfs://cluster/user/test/output/file-m-00002.lzo",
        "hdfs://cluster/user/test/output/file-m-00003.lzo",
        "hdfs://cluster/user/test/output/file-m-00003.lzo",
    };

    Configuration conf = new Configuration(false);
    conf.setInt(DistributedLzoIndexer.JOB_NAME_MAX_LENGTH_KEY, 50);
    Job job = new Job(conf);
    DistributedLzoIndexer.setJobName(job, args);

    String expected = DistributedLzoIndexer.DEFAULT_JOB_NAME_PREFIX + " [hdfs://cluster/user/test/...";
    // Truncated length should be 50 + 3 for the "..."
    assertEquals(53, expected.length());

    assertEquals(expected, job.getJobName());
  }

  public void testDisabledTruncation() throws Exception {
    String[] args = new String[]{
        "hdfs://cluster/user/test/output/file-m-00000.lzo",
        "hdfs://cluster/user/test/output/file-m-00001.lzo",
        "hdfs://cluster/user/test/output/file-m-00002.lzo",
        "hdfs://cluster/user/test/output/file-m-00003.lzo",
        "hdfs://cluster/user/test/output/file-m-00003.lzo",
    };

    Configuration conf = new Configuration(false);
    conf.setInt(DistributedLzoIndexer.JOB_NAME_MAX_LENGTH_KEY, 0);
    Job job = new Job(conf);
    DistributedLzoIndexer.setJobName(job, args);

    String expected = DistributedLzoIndexer.DEFAULT_JOB_NAME_PREFIX + " [hdfs://cluster/user/test/output/file-m-00000.lzo, hdfs://cluster/user/test/output/file-m-00001.lzo, hdfs://cluster/user/test/output/file-m-00002.lzo, hdfs://cluster/user/test/output/file-m-00003.lzo, hdfs://cluster/user/test/output/file-m-00003.lzo]";
    assertEquals(expected, job.getJobName());
  }

}