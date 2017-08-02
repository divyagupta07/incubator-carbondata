package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CarbonRecordWriter<T> extends RecordWriter<Void, T> {

  public CarbonRecordWriter(CarbonWriteSupport<T> writeSupport){

  }

  @Override public void write(Void key, T value) throws IOException, InterruptedException {

  }

  @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {

  }
}
