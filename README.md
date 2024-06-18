# Bigtable Change Streams

This repo has an example of using BT to BQ change streams for real time updates in BQ. It is used mainly to share the beam code used to build the playground model pipeline.


### Running
1. Compiling the Pipeline:
```
mvn compile exec:java -Dexec.mainClass=BigTableToBigQueryChangeStreams \             
-Dexec.args="--project=playground-259505  \
--runner=dataflow --region=us-west1 --experiments=use_runner_v2 --subnetwork=regions/us-west1/subnetworks/default
--serviceAccount=garrett-manaster-test@playground-259505.iam.gserviceaccount.com"
```
1. Loading Sample Data into `cdc-model`
```
cbt -instance=cdc-tutorial-garrett-manaster -project=playground-259505 import \                                   
cdc-model cc-transactions-extra-columns.csv  column-family=b  
```
