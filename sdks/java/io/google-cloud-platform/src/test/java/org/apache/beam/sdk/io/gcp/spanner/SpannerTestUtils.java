package org.apache.beam.sdk.io.gcp.spanner;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SpannerTestUtils {

  private static final int MAX_DB_NAME_LENGTH = 30;

  public interface SpannerTestPipelineOptions extends TestPipelineOptions {
    @Description("Project that hosts Spanner instance")
    @Nullable
    String getInstanceProjectId();

    void setInstanceProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("beam-test")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID prefix to write to in Spanner")
    @Default.String("beam-testdb")
    String getDatabaseIdPrefix();

    void setDatabaseIdPrefix(String value);

    @Description("Table name")
    @Default.String("users")
    String getTable();

    void setTable(String value);
  }

  public static String generateDatabaseName(String databaseIdPrefix) {
    String random =
        RandomUtils.randomAlphaNumeric(
            MAX_DB_NAME_LENGTH - 1 - databaseIdPrefix.length());
    return databaseIdPrefix + "-" + random;
  }
}
