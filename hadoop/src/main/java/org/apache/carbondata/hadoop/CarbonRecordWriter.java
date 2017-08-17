package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.carbondata.processing.model.CarbonLoadModel;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CarbonRecordWriter<T> extends RecordWriter<Void, T> {

  private final CarbonWriteSupport<T> writeSupport;

  public CarbonRecordWriter(CarbonLoadModel loadModel, CarbonWriteSupport<T> writeSupport)
      throws IOException {
    //handlesegmentmerging
    this.writeSupport = writeSupport;

  }

  @Override public void write(Void key, T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    //checkblocksizereached
  }

  @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {

  }
}
