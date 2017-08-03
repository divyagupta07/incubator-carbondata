package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.carbondata.processing.model.CarbonLoadModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class CarbonOutputFormat<T> extends OutputFormat<Void, T> {

  private static final String CARBON_WRITE_SUPPORT = "mapreduce.output.carbonoutputformat.writesupport";

  @Override public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration configuration = job.getConfiguration();
    CarbonLoadModel loadModel = getCarbonLoadModel(configuration);
    CarbonWriteSupport<T> writeSupport = getWriteSupport(configuration);
    //writeSupport.initialize();
    return new CarbonRecordWriter<>(loadModel, writeSupport);
  }

  @Override public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {

  }

  public CarbonLoadModel getCarbonLoadModel(Configuration configuration){
    CarbonLoadModel loadModel = new CarbonLoadModel();
    //create load model using values from configuration
    //set SinglePass to true
    return loadModel;
  }

  public CarbonWriteSupport<T> getWriteSupport(Configuration configuration) {
    String className = configuration.get(CARBON_WRITE_SUPPORT);

    if (className == null) {
      return null;
    }
    Class<?> writeSupportClass;
    try {
      writeSupportClass = configuration.getClassByName(className);

    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    try {
      if (writeSupportClass != null) {
        return ((CarbonWriteSupport) writeSupportClass.newInstance());
      } else return null;

    } catch (InstantiationException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static void setOutputPath(TaskAttemptContext context, Path outputDir) {
    context.getConfiguration().set("mapred.output.dir", outputDir.toString());
  }

  public static Path getOutputPath(TaskAttemptContext context) {
    String name = context.getConfiguration().get("mapred.output.dir");
    return name == null?null:new Path(name);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    Path output = getOutputPath(context);
      return new CarbonOutputCommiter(output, context);
  }
}
