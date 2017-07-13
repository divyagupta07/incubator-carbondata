package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class CarbonOutputCommiter extends FileOutputCommitter {
  public CarbonOutputCommiter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }
}
