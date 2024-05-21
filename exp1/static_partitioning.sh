#!/bin/bash

export BUCKET=""
export KEY=""
LINES_PER_FILE=12500000

aws s3 cp s3://$BUCKET/$KEY - | zcat - | split -l $LINES_PER_FILE --filter='aws s3 cp - s3://$BUCKET/$KEY-partitions/$FILE'
