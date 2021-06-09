package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;

public class PartitionMetadataDao {

  // Metadata table column names
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_PARENT_TOKEN = "ParentToken";
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  public static final String COLUMN_INCLUSIVE_START = "InclusiveStart";
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  public static final String COLUMN_INCLUSIVE_END = "InclusiveEnd";
  public static final String COLUMN_HEARTBEAT_SECONDS = "HeartbeatSeconds";
  public static final String COLUMN_STATE = "State";
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  public static final String COLUMN_UPDATED_AT = "UpdatedAt";

  private final DatabaseClient databaseClient;

  public PartitionMetadataDao (DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  public Timestamp insert(String table, PartitionMetadata partitionMetadata) {
    final Mutation mutation = insertMutationFrom(table, partitionMetadata);
    return databaseClient.write(ImmutableList.of(mutation));
  }

  public <T> T runInTransaction(String tableName, Function<InTransactionContext, T> callable) {
    return databaseClient
        .readWriteTransaction()
        .run(transaction -> {
          final InTransactionContext dao = new InTransactionContext(tableName, transaction);
          return callable.apply(dao);
        });
  }

  public static class InTransactionContext {

    private final String tableName;
    private final TransactionContext transaction;

    public InTransactionContext(String tableName, TransactionContext transaction) {
      this.tableName = tableName;
      this.transaction = transaction;
    }

    public void insert(List<PartitionMetadata> rows) {
      final List<Mutation> mutations = rows
          .stream()
          .map(row -> PartitionMetadataDao.insertMutationFrom(tableName, row))
          .collect(Collectors.toList());

      transaction.buffer(mutations);
    }

    public void updateState(String partitionToken, PartitionMetadata.State state) {
      final Mutation mutation = Mutation
          .newUpdateBuilder(tableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(COLUMN_STATE)
          .to(state.toString())
          // FIXME: This should be the pending commit timestamp
          // .set(COLUMN_UPDATED_AT)
          // .to(null)
          .build();
      transaction.buffer(mutation);
    }
  }

  private static Mutation insertMutationFrom(
      String tableName,
      PartitionMetadata partitionMetadata) {
    return Mutation.newInsertBuilder(tableName)
        .set(COLUMN_PARTITION_TOKEN)
        .to(partitionMetadata.getPartitionToken())
        // FIXME: This should be a list of parents
        .set(COLUMN_PARENT_TOKEN)
        .toStringArray(partitionMetadata.getParentTokens())
        .set(COLUMN_START_TIMESTAMP)
        .to(partitionMetadata.getStartTimestamp())
        .set(COLUMN_INCLUSIVE_START)
        .to(partitionMetadata.isInclusiveStart())
        .set(COLUMN_END_TIMESTAMP)
        .to(partitionMetadata.getEndTimestamp())
        .set(COLUMN_INCLUSIVE_END)
        .to(partitionMetadata.isInclusiveEnd())
        .set(COLUMN_HEARTBEAT_SECONDS)
        .to(partitionMetadata.getHeartbeatSeconds())
        .set(COLUMN_STATE)
        .to(partitionMetadata.getState().toString())
        // FIXME: This should be the pending commit timestamp
        .set(COLUMN_CREATED_AT)
        .to(partitionMetadata.getCreatedAt())
        // FIXME: This should be the pending commit timestamp
        .set(COLUMN_UPDATED_AT)
        .to(partitionMetadata.getUpdatedAt())
        .build();
  }
}
