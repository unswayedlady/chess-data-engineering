
#!/bin/bash

CLUSTER_ID=$1
MAIN_JAR=s3://tfg-chess-milagros/Queries.jar
MATCHES_PATH=s3://tfg-chess-milagros/matches.json
PLAYERS_PATH=s3://tfg-chess-milagros/players.json
OUTPUT_PATH=s3://tfg-chess-milagros/output.json
PROFILE=default

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="My program",ActionOnFailure=CONTINUE,Args=[--class,Queries,$MAIN_JAR,$MATCHES_PATH,$PLAYERS_PATH,$OUTPUT_PATH] \
  --profile $PROFILE \
  --region "us-east-1"
