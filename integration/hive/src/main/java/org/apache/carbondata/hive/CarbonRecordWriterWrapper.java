package org.apache.carbondata.hive;

import java.io.IOException;

import org.apache.carbondata.hadoop.CarbonOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import parquet.hadoop.util.ContextUtil;

public class CarbonRecordWriterWrapper implements RecordWriter<Void, ArrayWritable>,
    FileSinkOperator.RecordWriter {

  private final org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> realWriter;
  private final TaskAttemptContext taskContext;

  public CarbonRecordWriterWrapper( final OutputFormat<Void, ArrayWritable> realOutputFormat,
      final JobConf jobConf,
      final String name,
      final Progressable progress) throws IOException {
    try {
      // create a TaskInputOutputContext
      TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }
      taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);

      realWriter = (org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable>)
          ((CarbonOutputFormat) realOutputFormat).getRecordWriter(taskContext);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close(final Reporter reporter) throws IOException {
    try {
      realWriter.close(taskContext);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(final Void key, final ArrayWritable value) throws IOException {
    try {
      realWriter.write(key, value);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override public void write(Writable writable) throws IOException {
    write(null, (ArrayWritable) writable);
  }

  @Override public void close(boolean b) throws IOException {
    close(null);
  }
}
