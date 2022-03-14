package org.apache.beam.sdk.io.gcp.spanner.changestreams.generator;

public enum RecordAction {
  NONE,
  RECORD_NEW,
  RECORD_DELETE,
  RECORD_UPDATE
}