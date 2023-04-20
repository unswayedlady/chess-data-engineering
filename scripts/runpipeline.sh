
#!/bin/bash

CLUSTER_ID=$1
MAIN_JAR=s3://$2/Queries.jar
MATCHES_PATH=s3://$2/matches.json
PLAYERS_PATH=s3://$2/players.json
OUTPUT_PATH=s3://$2/output
PROFILE=default

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="My program",ActionOnFailure=CONTINUE,Args=[--class,main.Queries,$MAIN_JAR,"--inputMatchesPath",$MATCHES_PATH,"--inputPlayersPath",$PLAYERS_PATH,"--outputPath",$OUTPUT_PATH] \
  --profile $PROFILE \
  --region "us-east-1"
