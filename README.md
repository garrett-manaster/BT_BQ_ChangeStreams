# Bigtable Change Streams

This repo has an example of using BT to BQ change streams for real time updates in BQ. It is used mainly to share the beam code used to build the playground model pipeline.


### Running
1.
```
mvn compile exec:java -Dexec.mainClass=BigTableToBigQueryChangeStreams \             
-Dexec.args="--project=playground-259505  \
--runner=dataflow --region=us-west1 --experiments=use_runner_v2 --subnetwork=regions/us-west1/subnetworks/default
--serviceAccount=garrett-manaster-test@playground-259505.iam.gserviceaccount.com"
```
### Clean up
### Testing
## Running locally

To run either program on your local machine, you can use the direct Beam runner
by
setting `--runner=DirectRunner` (also the default if not specified). If you're running it locally, you don't need the
`--project` or `--region` parameters.
