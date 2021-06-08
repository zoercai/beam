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

package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;

// TODO: Check if this should be made static
public class TestStructMapper {

  public static final Type COLUMN_TYPE_TYPE = Type.struct(
      StructField.of("name", Type.string()),
      StructField.of("type", Type.string()),
      StructField.of("is_primary_key", Type.bool()),
      StructField.of("ordinal_position", Type.int64())
  );
  public static final Type MOD_TYPE = Type.struct(
      StructField.of("keys", Type.string()),
      StructField.of("new_values", Type.string()),
      StructField.of("old_values", Type.string())
  );
  private static final Type DATA_CHANGE_RECORD_TYPE = Type.struct(
      StructField.of("commit_timestamp", Type.timestamp()),
      StructField.of("record_sequence", Type.string()),
      StructField.of("server_transaction_id", Type.string()),
      StructField.of("is_last_record_in_transaction_in_partition", Type.bool()),
      StructField.of("table_name", Type.string()),
      StructField.of("column_types", Type.array(COLUMN_TYPE_TYPE)),
      StructField.of("mods", Type.array(MOD_TYPE)),
      StructField.of("mod_type", Type.string()),
      StructField.of("value_capture_type", Type.string()),
      StructField.of("number_of_records_in_transaction", Type.int64()),
      StructField.of("number_of_partitions_in_transaction", Type.int64())
  );
  private static final Type HEARTBEAT_RECORD_TYPE = Type.struct(
      StructField.of("timestamp", Type.timestamp())
  );
  private static final Type CHILD_PARTITIONS_RECORD_TYPE = Type.struct(
      StructField.of("start_timestamp", Type.timestamp()),
      StructField.of("record_sequence", Type.string()),
      StructField.of("child_partitions", Type.array(Type.struct(
          StructField.of("token", Type.string()),
          StructField.of("parent_partitions_token", Type.array(Type.string()))
      )))
  );
  private static final Type STREAM_RECORD_TYPE = Type.struct(
      StructField.of("data_change_record", DATA_CHANGE_RECORD_TYPE),
      StructField.of("heartbeat_record", HEARTBEAT_RECORD_TYPE),
      StructField.of("child_partitions_record", CHILD_PARTITIONS_RECORD_TYPE)
  );
  private static final Gson gson = new Gson();

  public static Struct recordsToStruct(ChangeStreamRecord... records) {
    return Struct
        .newBuilder()
        .add(Value.structArray(
            STREAM_RECORD_TYPE,
            Arrays.stream(records).map(TestStructMapper::streamRecordStructFrom).collect(Collectors.toList())
        ))
        .build();
  }

  private static Struct streamRecordStructFrom(ChangeStreamRecord record) {
    if (record instanceof DataChangesRecord) {
      return streamRecordStructFrom((DataChangesRecord) record);
    } else if (record instanceof HeartbeatRecord) {
      return streamRecordStructFrom((HeartbeatRecord) record);
    } else {
      throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
    }
  }

  private static Struct streamRecordStructFrom(HeartbeatRecord record) {
    return Struct
        .newBuilder()
        .set("data_change_record")
        .to(DATA_CHANGE_RECORD_TYPE, null)
        .set("heartbeat_record")
        .to(HEARTBEAT_RECORD_TYPE, recordStructFrom(record))
        .set("child_partitions_record")
        .to(CHILD_PARTITIONS_RECORD_TYPE, null)
        .build();

  }

  private static Struct recordStructFrom(HeartbeatRecord record) {
    return Struct
        .newBuilder()
        .set("timestamp")
        .to(record.getTimestamp())
        .build();
  }

  private static Struct streamRecordStructFrom(DataChangesRecord record) {
    return Struct
        .newBuilder()
        .set("data_change_record")
        .to(DATA_CHANGE_RECORD_TYPE, recordStructFrom(record))
        .set("heartbeat_record")
        .to(HEARTBEAT_RECORD_TYPE, null)
        .set("child_partitions_record")
        .to(CHILD_PARTITIONS_RECORD_TYPE, null)
        .build();
  }

  private static Struct recordStructFrom(DataChangesRecord record) {
    final Value columnTypes = Value.structArray(
        COLUMN_TYPE_TYPE,
        record.getRowType().stream().map(TestStructMapper::columnTypeStructFrom).collect(Collectors.toList())
    );
    final Value mods = Value.structArray(
        MOD_TYPE,
        record.getMods().stream().map(TestStructMapper::modStructFrom).collect(Collectors.toList())
    );
    return Struct
        .newBuilder()
        .set("commit_timestamp")
        .to(record.getCommitTimestamp())
        .set("record_sequence")
        .to(record.getRecordSequence())
        .set("server_transaction_id")
        .to(record.getTransactionId())
        .set("is_last_record_in_transaction_in_partition")
        .to(record.isLastRecordInTransactionPartition())
        .set("table_name")
        .to(record.getTableName())
        .set("column_types")
        .to(columnTypes)
        .set("mods")
        .to(mods)
        .set("mod_type")
        .to(record.getModType().toString())
        .set("value_capture_type")
        .to(record.getValueCaptureType().toString())

        // FIXME: Record should have number of records in transaction
        .set("number_of_records_in_transaction")
        .to(-1)

        // FIXME: Record should have number of partitions in transaction
        .set("number_of_partitions_in_transaction")
        .to(-1)

        .build();
  }

  private static Struct columnTypeStructFrom(ColumnType columnType) {
    return Struct
        .newBuilder()
        .set("name")
        .to(columnType.getName())
        .set("type")
        .to(columnType.getType().getCode())
        .set("is_primary_key")
        .to(columnType.isPrimaryKey())
        .set("ordinal_position")

        // FIXME: ColumnType should have an ordinal position
        .to(0)

        .build();
  }

  private static Struct modStructFrom(Mod mod) {
    // FIXME: Mod should have a keys array
    final String keys = "";

    final String newValues = gson.toJson(mod.getNewValues());
    final String oldValues = gson.toJson(mod.getOldValues());
    return Struct
        .newBuilder()
        .set("keys")
        .to(keys)
        .set("new_values")
        .to(newValues)
        .set("old_values")
        .to(oldValues)
        .build();
  }

}
