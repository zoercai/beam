package org.apache.beam.sdk.io.gcp.spanner.changestreams.generator;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Mapper {
  private static final Table TABLE = Table.of("Table");

  private static final List<ColumnType> ROW_TYPE =
      Arrays.asList(
          new ColumnType("Id", new TypeCode("INT64"), true),
          new ColumnType("SongName", new TypeCode("STRING(MAX)"), false));

  public static String mapPartitionToken(long tokenId) {
    return "Partition-" + tokenId;
  }

  public static PartitionRecord mapPartitionRecord(
      String partitionToken,
      long recordSequence,
      Set<String> oldPartitions,
      Set<String> newPartitions) {
    return new PartitionRecord(
        partitionToken,
        recordSequence,
        Timestamp.now(),
        oldPartitions,
        newPartitions);
  }

  public static DataChangesRecord mapChangeRecord(
      RecordAction recordAction,
      Partition partition,
      List<Integer> primaryKeys,
      long transactionId,
      long recordSequence) {
    List<Mod> mods =
        primaryKeys.stream().map(key -> mapMod(recordAction, key)).collect(Collectors.toList());
    return new DataChangesRecord(
        partition.getPartitionId(),
        Timestamp.of(System.currentTimeMillis()),
        TransactionId.of(transactionId),
        true,
        RecordSequence.of(recordSequence),
        TABLE,
        ROW_TYPE,
        mods,
        mapModType(recordAction),
        mapValueCaptureType(recordAction));
  }

  private static Mod mapMod(RecordAction recordAction, Integer key) {
    switch (recordAction) {
      case RECORD_NEW:
        return Mod.newNewBuilder()
            .addValue("Id", key.toString())
            .addValue("SongName", "Just created Song")
            .build();
      case RECORD_DELETE:
        return Mod.newOldBuilder()
            .addValue("Id", key.toString())
            .addValue("SongName", "Deleted Song")
            .build();
      case RECORD_UPDATE:
        return Mod.newOldAndNewBuilder()
            .addValue("Id", key.toString(), key.toString())
            .addValue("SongName", "Old Song " + key, "New Song " + key)
            .build();
    }
    return null;
  }

  public static ModType mapModType(RecordAction recordAction) {
    switch (recordAction) {
      case RECORD_NEW:
        return ModType.INSERT;
      case RECORD_DELETE:
        return ModType.DELETE;
      case RECORD_UPDATE:
        return ModType.UPDATE;
    }
    return null;
  }

  public static ValueCaptureType mapValueCaptureType(RecordAction recordAction) {
    if (recordAction == RecordAction.RECORD_NEW) {
      return ValueCaptureType.NEW_VALUES;
    }
    // TODO add deletion?
    return ValueCaptureType.OLD_AND_NEW_VALUES;
  }
}