/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

public class ChangeStreamDao {

  private final String changeStreamName;
  private final DatabaseClient databaseClient;

  public ChangeStreamDao(
      String changeStreamName,
      DatabaseClient databaseClient) {
    this.changeStreamName = changeStreamName;
    this.databaseClient = databaseClient;
  }

  public ResultSet changeStreamQuery(
      String partitionToken,
      Timestamp startTimestamp,
      boolean isInclusiveStart,
      Timestamp endTimestamp,
      boolean isInclusiveEnd,
      long heartbeatSeconds
  ) {
    final String startTimeOptions = isInclusiveStart ?
        "INCLUDE_START_TIME" : "EXCLUDE_START_TIME";
    final String endTimeOptions = isInclusiveEnd ?
        "INCLUDE_END_TIME" : "EXCLUDE_END_TIME";

    return databaseClient
        .singleUse()
        .executeQuery(Statement.of("SELECT *"
            + " FROM READ_" + changeStreamName + "("
            + " partitionToken => '" + partitionToken + "',"
            + " start_timestamp => '" + startTimestamp + "',"
            + " end_timestamp => '" + endTimestamp + "',"
            + " read_options => [" + startTimeOptions + "," + endTimeOptions + "],"
            + " heartbeat_seconds => " + heartbeatSeconds
            + ")"
        ));
  }
}
