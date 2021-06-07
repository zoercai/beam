package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TestStructMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamRecordMapperTest {

  private ChangeStreamRecordMapper mapper;
  private TestStructMapper testStructMapper;

  @Before
  public void setUp() throws Exception {
    this.mapper = new ChangeStreamRecordMapper(new Gson());
    this.testStructMapper = new TestStructMapper();
  }

  @Test
  public void testMappingStructRowToDataChangesRecord() {
    final DataChangesRecord dataChangesRecord = new DataChangesRecord(
        "partitionToken",
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        "transactionId",
        true,
        "recordSequence",
        "tableName",
        Arrays.asList(
            new ColumnType("column1", new TypeCode("type1"), true),
            new ColumnType("column2", new TypeCode("type2"), false)
        ),
        Collections.singletonList(
            new Mod(
                ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
                ImmutableMap.of("column1", "value1", "column2", "newValue2")
            )
        ),
        ModType.UPDATE,
        ValueCaptureType.OLD_AND_NEW_VALUES
    );
    final Struct struct = testStructMapper.toStruct(dataChangesRecord);

    assertEquals(
        Collections.singletonList(dataChangesRecord),
        mapper.toChangeStreamRecords("partitionToken", struct)
    );
  }

  // TODO: Add test for heatbeat record
  // TODO: Add test for child partitions record
}
