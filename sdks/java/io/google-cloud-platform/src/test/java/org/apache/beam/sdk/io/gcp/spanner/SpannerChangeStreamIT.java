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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamIT {

  public static final String SPANNER_HOST = "https://staging-wrenchworks.sandbox.googleapis.com";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends IOTestPipelineOptions, StreamingOptions {

    @Description("Project that hosts Spanner instance")
    @Nullable
    String getProjectId();

    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("change-stream-load-test-3")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID prefix to write to in Spanner")
    @Default.String("load-test-change-stream-enable")
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Default.Integer(300)
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);
  }

  private static final String CHANGE_STREAM_NAME = "changeStreamAll";
  private static final String METADATA_DATABASE = "change-stream-metadata";
  private static SpannerTestPipelineOptions options;
  private static String projectId;
  private static String instanceId;
  private static String databaseId;
  private static Spanner spanner;

  @BeforeClass
  public static void setup() throws InterruptedException, ExecutionException, TimeoutException {
    options = IOITHelper.readIOTestPipelineOptions(SpannerTestPipelineOptions.class);
    projectId =
        options.getProjectId() == null
            ? options.as(GcpOptions.class).getProject()
            : options.getProjectId();
    instanceId = options.getInstanceId();
    databaseId = options.getDatabaseId();
    spanner =
        SpannerOptions.newBuilder()
            .setHost(SPANNER_HOST)
            .setProjectId(projectId)
            .build()
            .getService();
  }

  @AfterClass
  public static void afterClass()
      throws InterruptedException, ExecutionException, TimeoutException {
    spanner.close();
  }

  @Test
  public void testReadSpannerChangeStream() {
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(StaticValueProvider.of(SPANNER_HOST))
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    final Timestamp startTime = Timestamp.now();
    final Timestamp endTime =
        Timestamp.ofTimeSecondsAndNanos(startTime.getSeconds() + 60, startTime.getNanos());

    pipeline.getOptions().as(SpannerTestPipelineOptions.class).setStreaming(true);
    pipeline.getOptions().as(SpannerTestPipelineOptions.class).setBlockOnRun(false);

    pipeline.apply(
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(CHANGE_STREAM_NAME)
            .withMetadataDatabase(METADATA_DATABASE)
            .withMetadataDatabase(databaseId)
            .withInclusiveStartAt(startTime)
            .withInclusiveEndAt(endTime));

    pipeline.run().waitUntilFinish();
  }
}
