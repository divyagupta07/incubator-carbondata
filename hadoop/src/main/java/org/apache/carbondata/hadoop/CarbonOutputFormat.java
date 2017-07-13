package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class CarbonOutputFormat<T> extends FileOutputFormat<Void, T> {

  @Override public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return new CarbonRecordWriter<>();
  }

  public CarbonWriteSupport<T> getWriteSupportClass(Configuration configuration) {
    return new CarbonWriteSupport();
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
