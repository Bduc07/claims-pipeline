#!/usr/bin/env bash
set -euo pipefail

scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
jars="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / "jars")')"
demo="${CLAIMS_PIPELINE_DATA:-/opt/airflow/data}/output/cloud_demo"

java -cp "/opt/airflow/hadoop-streaming.jar:$jars/*" org.apache.hadoop.streaming.HadoopStreaming \
    -D mapreduce.framework.name=local -D fs.defaultFS=file:/// -D mapreduce.job.reduces=1 \
    -input "$demo/mapreduce_input" -output "${1:-$demo/mapreduce_output}" \
    -mapper "python $scripts/payment_mapper.py" -reducer "python $scripts/payment_reducer.py"
