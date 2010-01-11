package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LzoSplitInputFormat extends FileInputFormat<Path, LongWritable> {

  @Override
  public RecordReader<Path, LongWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new LzoSplitRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // Force the files to be unsplittable, because indexing requires seeing all the
    // compressed blocks in succession.
    return false;
  }
}
