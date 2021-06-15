package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;

// TODO: Integration test
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

  // TODO: We should receive a table name in the constructor
  public PartitionMetadataDao (DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  // TODO: Implement
  // This should be the query as in https://docs.google.com/document/d/1SxD3L46vlAtAdC2wEFjhYuGuddE3AvkB1HFRnQ3yrzs/edit?resourcekey=0-k6ifYSXSlw0UQLRtVwnLlQ#bookmark=id.q712q48z4rl
  public long countChildPartitionsInStates(
      String partitionToken,
      List<PartitionMetadata.State> states) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  // TODO: Implement
  // This should be the query as in https://docs.google.com/document/d/1SxD3L46vlAtAdC2wEFjhYuGuddE3AvkB1HFRnQ3yrzs/edit?resourcekey=0-k6ifYSXSlw0UQLRtVwnLlQ#
  public long countExistingParents(String partitionToken) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  // FIXME: This should be unified with the method below (without the stable name as a parameter)
  public void insert(String table, PartitionMetadata partitionMetadata) {
    runInTransaction(transaction -> transaction.insert(partitionMetadata));
  }

  // FIXME: This should be unified with the method above
  public void insert(PartitionMetadata row) {
    runInTransaction(transaction -> transaction.insert(row));
  }

  public long updateState(
      String partitionToken,
      PartitionMetadata.State state) {
    return runInTransaction(transaction -> transaction.updateState(partitionToken, state));
  }

  public long delete(String partitionToken) {
    return runInTransaction(transaction -> transaction.delete(partitionToken));
  }

  public <T> T runInTransaction(Function<InTransactionContext, T> callable) {
    return databaseClient
        .readWriteTransaction()
        .run(transaction -> {
          // FIXME: table name should be given in the constructor instead of null
          final InTransactionContext dao = new InTransactionContext(null, transaction);
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

    // TODO: Check implementation
    public Void insert(PartitionMetadata row) {
      transaction.buffer(insertMutationFrom(tableName, row));
      return null;
    }

    // TODO: Implement
    public long updateState(String partitionToken, PartitionMetadata.State state) {
      throw new UnsupportedOperationException("Unimplemented");
    }

    // TODO: Implement
    public long delete(String partitionToken) {
      throw new UnsupportedOperationException("Unimplemented");
    }

    // TODO: Implement
    public long countPartitionsInStates(
        List<String> partitionTokens,
        List<PartitionMetadata.State> states
    ) {
      throw new UnsupportedOperationException("Unimplemented");
    }

    private Mutation insertMutationFrom(
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
          .set(COLUMN_CREATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .set(COLUMN_UPDATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }
  }
}
