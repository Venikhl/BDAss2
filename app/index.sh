#!/bin/bash

HDFS_SOURCE_DIR="/data"
LOCAL_DIR="/app/mapreduce"
MAPPER_OUTPUT="/tmp/index-output"
LOCAL_MAPPER_RESULT="/tmp/mapper_output.txt"

HDFS_INPUT=$(hdfs dfs -ls "$HDFS_SOURCE_DIR" | tail -n +2 | sed 's/^.* \(\/.*\)$/\1/' | head -n 100 | tr '\n' ',' | sed 's/,$//')


hdfs dfs -rm -r -f "$MAPPER_OUTPUT"

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=0 \
  -input "$HDFS_INPUT" \
  -output "$MAPPER_OUTPUT" \
  -mapper "mapper1.py" \
  -file "$LOCAL_DIR/mapper1.py"


MAPPER_OUTPUT_FILE="$MAPPER_OUTPUT/part-*"
if hdfs dfs -test -e "$MAPPER_OUTPUT_FILE"; then
  hdfs dfs -cat "$MAPPER_OUTPUT_FILE" > "$LOCAL_MAPPER_RESULT"
else
  echo "NO Mapper!"
  exit 1
fi

sort "$LOCAL_MAPPER_RESULT" | python3 "$LOCAL_DIR/reducer1.py"
echo "Finished"
