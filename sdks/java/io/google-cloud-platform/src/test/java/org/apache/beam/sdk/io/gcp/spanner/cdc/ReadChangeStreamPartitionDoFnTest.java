package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.spanner.DatabaseClient;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel.PartitionId;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class ReadChangeStreamPartitionDoFnTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void test() {
    final TestStream<PartitionId> testStream = TestStream
        .create(AvroCoder.of(PartitionId.class))
        .addElements(new PartitionId(1L))
        .advanceWatermarkToInfinity();
    final SpannerAccessor spannerAccessor = Mockito.mock(SpannerAccessor.class);
    final ReadChangeStreamPartitionDoFn doFn = new ReadChangeStreamPartitionDoFn(null,
        spannerAccessor);

    pipeline
        .apply(testStream)
        .apply(ParDo.of(doFn));

    pipeline.run().waitUntilFinish();
  }
}
