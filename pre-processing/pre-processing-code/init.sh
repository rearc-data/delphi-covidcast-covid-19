#!/usr/bin/env bash

while [[ $# -gt 0 ]]; do
  opt="${1}"
  shift;
  current_arg="$1"
  case ${opt} in
    "-n"|"--data-set-name") export DATA_SET_NAME="$1"; shift;;
    "-a"|"--data-set-arn") export DATA_SET_ARN="$1"; shift;;
    "-i"|"--product-id") export PRODUCT_ID="$1"; shift;;
    "-r"|"--region") export REGION="$1"; shift;;
    "-s"|"--s3-bucket") export S3_BUCKET="$1"; shift;;
    *) echo "ERROR: Invalid option: \""$opt"\"" >&2; exit 1;;
  esac
done

python lambda_function.py