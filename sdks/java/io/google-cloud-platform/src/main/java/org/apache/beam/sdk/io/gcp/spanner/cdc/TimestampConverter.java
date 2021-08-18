/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.math.RoundingMode;

// TODO: javadocs
// TODO: This class probably does not belong in the cdc change
public class TimestampConverter {

  private static final BigDecimal TEN_TO_THE_THIRD = BigDecimal.ONE.scaleByPowerOfTen(3);
  private static final BigDecimal TEN_TO_THE_SIXTH = BigDecimal.ONE.scaleByPowerOfTen(6);
  private static final BigDecimal TEN_TO_THE_NINTH = BigDecimal.ONE.scaleByPowerOfTen(9);

  public static BigDecimal timestampToNanos(Timestamp timestamp) {
    final BigDecimal seconds = BigDecimal.valueOf(timestamp.getSeconds());
    final BigDecimal nanos = BigDecimal.valueOf(timestamp.getNanos());

    return seconds.multiply(TEN_TO_THE_NINTH).add(nanos);
  }

  public static Timestamp timestampFromNanos(BigDecimal timestampAsNanos) {
    final long seconds = timestampAsNanos.divide(TEN_TO_THE_NINTH, RoundingMode.FLOOR).longValue();
    final int nanos = timestampAsNanos.remainder(TEN_TO_THE_NINTH).intValue();

    return Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
  }

  public static BigDecimal timestampToMicros(Timestamp timestamp) {
    final BigDecimal seconds = BigDecimal.valueOf(timestamp.getSeconds());
    final BigDecimal nanos = BigDecimal.valueOf(timestamp.getNanos());
    final BigDecimal micros = nanos.divide(TEN_TO_THE_THIRD, RoundingMode.FLOOR);

    return seconds.multiply(TEN_TO_THE_SIXTH).add(micros);
  }

  public static Timestamp timestampFromMicros(BigDecimal timestampAsMicros) {
    return Timestamp.ofTimeMicroseconds(timestampAsMicros.longValue());
  }

  public static Timestamp timestampFromMillis(long millis) {
    return Timestamp.ofTimeMicroseconds(millis * TEN_TO_THE_THIRD.longValue());
  }
}
