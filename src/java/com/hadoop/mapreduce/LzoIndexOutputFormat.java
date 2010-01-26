package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LzoIndexOutputFormat extends OutputFormat<Path, LongWritable> {
  @Override
  public RecordWriter<Path, LongWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new LzoIndexRecordWriter(taskAttemptContext);
  }

  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {}

  // A totally no-op output committer, because the LzoIndexRecordWriter opens a file on the side
  // and writes to that instead.
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new OutputCommitter() {
      @Override public void setupJob(JobContext jobContext) throws IOException {}
      @Override public void cleanupJob(JobContext jobContext) throws IOException {}
      @Override public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {}
      @Override public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {}
      @Override public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {}
      @Override public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
      }
    };
  }
}
