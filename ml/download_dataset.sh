#!/bin/bash
set -e
mkdir -p ml/data
kaggle datasets download -d mlg-ulb/creditcardfraud --path ml/data --unzip
echo "Dataset ready:"
wc -l ml/data/creditcard.csv
