import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.api.services.bigquery.model.TableRow;
import common.BTtoBQPipelineOptions;
import common.WindowUtils.DurationUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import com.google.protobuf.ByteString;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;

import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.Duration;

public class BigTableToBigQueryChangeStreams {

  public static final Logger LOG = LoggerFactory.getLogger(BigTableToBigQueryChangeStreams.class);


  public static void main(String[] args) {
    // [START bigtable_cdc_hw_pipeline]
    BTtoBQPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BTtoBQPipelineOptions.class);
    Pipeline p = Pipeline.create(options);
    String deadletter_bucket_path = options.getDeadletterGCSBucket();

    final Instant startTime = Instant.now();

    PCollection<KV<ByteString, ChangeStreamMutation>> extract = p.apply(
            "Extract: Read Change Stream",
            BigtableIO.readChangeStream()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .withAppProfileId(options.getBigtableAppProfile())
                .withStartTime(startTime));

    PCollection<TableRow> rows = extract.apply("Transform: MutationPair -> TableRow",
                    ParDo.of(new ConvertToTableRowFn()));

    WriteResult result = rows.apply("Load: Write to BigQuery",
            BigQueryIO.writeTableRows()
                    .to(String.format("%s:%s.%s", options.getBigQueryProjectID(), options.getBigQueryDataset(), options.getBigQueryTableID()))
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                    .withExtendedErrorInfo()
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND));


    PCollection<String> errorRows = result.getFailedInsertsWithErr()
            .apply("Log Error",
                    MapElements.into(TypeDescriptors.strings())
                            .via(
                                    x -> {
                                      LOG.error("Error: {}\n Recover error at {deadletter_buckeyt}/deadletter/CDC-Error-...", x.getError());
                                      LOG.debug("Table with Error: {}", x.getTable());
                                      LOG.debug("Row with error: {}", x.getRow());
                                      return x.getRow().toString();
                                    }));


    Duration windowingDuration = DurationUtils.parseDuration(options.getDeadletterWindowDuration());

    errorRows.apply(
            "Window each minute of errors",
            Window.<String>into(new GlobalWindows())
                    .triggering(
                            Repeatedly.forever(
                                    AfterProcessingTime
                                            .pastFirstElementInPane()
                                            .plusDelayOf(windowingDuration)
                                    )
                            )
                    .discardingFiredPanes())
            .apply("Write Errors to GCS",
                    TextIO.write()
                            .to(options.getDeadletterGCSBucket() + "deadletter/CDC-Error" + Instant.now())
                            .withSuffix(".txt")
                            .withNumShards(1)
                            .withWindowedWrites()
                    );
    p.run();
  }

  // DoFn that convert the mutation into a bq TableRow. This is for set operations only.
  public static class ConvertToTableRowFn extends DoFn<KV<ByteString, ChangeStreamMutation>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<ByteString, ChangeStreamMutation> mutationPair = c.element();
      TableRow row = new TableRow();

      assert mutationPair != null && mutationPair.getKey() != null;
      row.set("key", mutationPair.getKey().toStringUtf8());

      row.set("timestamp", Instant.now());

      ChangeStreamMutation mutation = mutationPair.getValue();

      assert mutation != null;
      for (Entry entry : mutation.getEntries()) {
        row.set(((SetCell) entry).getQualifier().toStringUtf8(), ((SetCell) entry).getValue().toStringUtf8());
      }

      LOG.info("Row Converted: {}", row);
      c.output(row);
    }
  }

}
