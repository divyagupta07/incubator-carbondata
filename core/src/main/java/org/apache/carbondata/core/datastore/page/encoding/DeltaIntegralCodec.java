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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.CodecMetaFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class DeltaIntegralCodec extends AdaptiveCompressionCodec {

  private ColumnPage encodedPage;

  private long max;

  public static DeltaIntegralCodec newInstance(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, Compressor compressor) {
    return new DeltaIntegralCodec(srcDataType, targetDataType, stats, compressor);
  }

  private DeltaIntegralCodec(DataType srcDataType, DataType targetDataType, SimpleStatsResult stats,
      Compressor compressor) {
    super(srcDataType, targetDataType, stats, compressor);
    switch (srcDataType) {
      case BYTE:
        max = (byte) stats.getMax();
        break;
      case SHORT:
        max = (short) stats.getMax();
        break;
      case INT:
        max = (int) stats.getMax();
        break;
      case LONG:
        max = (long) stats.getMax();
        break;
      case FLOAT:
      case DOUBLE:
        max = (long) ((double) stats.getMax());
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for Delta compress: " + srcDataType);
    }
  }

  @Override
  public String getName() {
    return "DeltaIntegralCodec";
  }

  @Override
  public EncodedColumnPage encode(ColumnPage input) throws MemoryException, IOException {
    encodedPage = ColumnPage
        .newPage(targetDataType, input.getPageSize(), stats.getScale(), stats.getPrecision());
    input.encode(codec);
    byte[] result = encodedPage.compress(compressor);
    encodedPage.freeMemory();
    return new EncodedMeasurePage(input.getPageSize(),
        result,
        CodecMetaFactory.createMeta(stats, targetDataType),
        ((SimpleStatsResult)input.getStatistics()).getNullBits());
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length,
        stats.getScale(), stats.getPrecision());
    return LazyColumnPage.newPage(page, codec);
  }

  // encoded value = (max value of page) - (page value)
  private PrimitiveCodec codec = new PrimitiveCodec() {
    @Override
    public void encode(int rowId, byte value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, max - value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public long decodeLong(byte value) {
      return max - value;
    }

    @Override
    public long decodeLong(short value) {
      return max - value;
    }

    @Override
    public long decodeLong(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(byte value) {
      return max - value;
    }

    @Override
    public double decodeDouble(short value) {
      return max - value;
    }

    @Override
    public double decodeDouble(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(long value) {
      return max - value;
    }

    @Override
    public double decodeDouble(float value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error: " + debugInfo());
    }
  };
}
