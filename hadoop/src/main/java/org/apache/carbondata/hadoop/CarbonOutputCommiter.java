package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CarbonOutputCommiter extends OutputCommitter {
  public CarbonOutputCommiter(Path outputPath, TaskAttemptContext context) throws IOException {

  }

  @Override public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override public void setupTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  @Override public void commitTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override public void abortTask(TaskAttemptContext taskContext) throws IOException {

  }
}
