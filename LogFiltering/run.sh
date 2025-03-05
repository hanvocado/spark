#!/bin/bash
rm -rf output

if [ -z "$1" ]; then
    INPUT_PATH="src/main/resources/log.txt"
else
    INPUT_PATH="$1"
fi

if [ -z "$2" ]; then
    OUTPUT_PATH="output"
else
    OUTPUT_PATH="$2"
fi

echo "Using input path: $INPUT_PATH"
echo "Using output dir: $OUTPUT_PATH"

spark-submit --class log.spark.FilterIP \
    --master local[*] \
    target/LogFiltering-0.0.1-SNAPSHOT.jar "$INPUT_PATH" "$OUTPUT_PATH"