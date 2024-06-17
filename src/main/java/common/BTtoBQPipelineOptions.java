package common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface BTtoBQPipelineOptions extends DataflowPipelineOptions {

    @Description("The Bigtable project ID, this can be different than your Dataflow and BQ project")
    @Default.String("playground-259505")
    String getBigtableProjectId();

    void setBigtableProjectId(String bigtableProjectId);

    @Description("The Bigtable instance ID")
    @Default.String("cdc-tutorial-garrett-manaster")
    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    @Description("The Bigtable table ID in the instance.")
    @Default.String("cdc-model")
    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);

    @Description("The Bigtable application profile in the instance.")
    @Default.String("default")
    String getBigtableAppProfile();

    void setBigtableAppProfile(String bigtableAppProfile);

    @Description("The BigQuery Dataset.")
    @Default.String("garrett_manaster")
    String getBigQueryDataset();
    void setBigQueryDataset(String bigQueryDataset);

    @Description("The BigQuery Table.")
    @Default.String("cdc-model")
    String getBigQueryTableID();
    void setBigQueryTableID(String bigQueryTableID);

    @Description("The BigQuery Project.")
    @Default.String("playground-259505")
    String getBigQueryProjectID();
    void setBigQueryProjectID(String bigQueryProjectID);

    @Description("The Deadletter GCS Bucket.")
    @Default.String("gs://garrett_manaster_cdc_bucket/")
    String getDeadletterGCSBucket();
    void setDeadletterGCSBucket(String deadletterGCSBucket);

    @Description("The Window Duration for Writes to GCS")
    @Default.String("30s")
    String getDeadletterWindowDuration();
    void setDeadletterWindowDuration(String deadletterWindowDuration);
}