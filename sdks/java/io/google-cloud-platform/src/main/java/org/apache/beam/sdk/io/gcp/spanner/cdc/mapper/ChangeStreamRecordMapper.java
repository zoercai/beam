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

package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;

// TODO: Add unit tests
public class ChangeStreamRecordMapper {

  private final Gson gson;

  public ChangeStreamRecordMapper(Gson gson) {
    this.gson = gson;
  }

  public List<ChangeStreamRecord> toChangeStreamRecords(String partitionToken, Struct row) {
    return row
        .getStructList(0)
        .stream()
        .map(struct -> toChangeStreamRecord(partitionToken, struct))
        .collect(Collectors.toList());
  }

  private ChangeStreamRecord toChangeStreamRecord(String partitionToken, Struct row) {
    return toDataChangesRecord(partitionToken, row.getStruct("data_change_record"));
  }

  private DataChangesRecord toDataChangesRecord(String partitionToken, Struct row) {
    return new DataChangesRecord(
        partitionToken,
        row.getTimestamp("commit_timestamp"),
        row.getString("server_transaction_id"),
        row.getBoolean("is_last_record_in_transaction_in_partition"),
        row.getString("record_sequence"),
        row.getString("table_name"),
        row.getStructList("column_types").stream().map(this::columnTypeFrom).collect(Collectors.toList()),
        row.getStructList("mods").stream().map(this::modFrom).collect(Collectors.toList()),
        ModType.valueOf(row.getString("mod_type")),
        ValueCaptureType.valueOf(row.getString("value_capture_type"))
    );
  }

  private ColumnType columnTypeFrom(Struct struct) {
    return new ColumnType(
        struct.getString("name"),
        new TypeCode(struct.getString("type")),
        struct.getBoolean("is_primary_key")
    );
  }

  private Mod modFrom(Struct struct) {
    final Map<String, String> oldValues = gson.fromJson(struct.getString("old_values"), Map.class);
    final Map<String, String> newValues = gson.fromJson(struct.getString("new_values"), Map.class);
    return new Mod(
        oldValues,
        newValues
    );
  }
}
