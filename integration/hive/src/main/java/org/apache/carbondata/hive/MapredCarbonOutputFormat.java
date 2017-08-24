/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.carbondata.hadoop.CarbonOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 * TODO : To extend CarbonOutputFormat
 */
class MapredCarbonOutputFormat<T> extends CarbonOutputFormat<T>
    implements HiveOutputFormat<Void, T> {

  protected CarbonOutputFormat<ArrayWritable> realOutputFormat;

  public MapredCarbonOutputFormat() {
    realOutputFormat = new CarbonOutputFormat<>();
  }

  @Override
  public RecordWriter<Void, T> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
      Progressable progressable) throws IOException {
    return null;
  }

  @Override public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {

  }

  @Override public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {

    JobConf properties = new JobConf();
    properties.set("carbondata.current.database",jc.get("hive.current.database"));
    //Extract database_name and tablename from location
    properties.set("carbondata.current.location",jc.get("location"));
    properties.set("carbondata.schema.dataTypes",jc.get("columns.types"));
    properties.set("carbondata.schema.columnNames",jc.get("columns"));
    properties.set("carbondata.serde.lib",jc.get("serialization.lib"));
    properties.set("carbondata.input.format",jc.get("file.inputformat"));

    return new CarbonRecordWriterWrapper(realOutputFormat, properties, finalOutPath.toString(), progress,
        tableProperties);
  }
}
