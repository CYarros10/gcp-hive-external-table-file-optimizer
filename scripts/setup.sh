export PROJECT_ID=$1
export PROJECT_NUMBER=$2
export REGION=$3
export DATASET=$4
export TABLE=$5
export CLUSTER_NAME=$PROJECT_ID-dp-cluster
export GCS_BUCKET_NAME=$PROJECT_ID-services

gcloud config set project $PROJECT_ID

gcloud services enable dataproc.googleapis.com \
  compute.googleapis.com \
  storage-component.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com

gcloud beta dataproc clusters create $CLUSTER_NAME-hive \
--enable-component-gateway \
--optional-components=JUPYTER,ANACONDA \
--service-account=$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
--region $REGION --subnet default --zone $REGION-a \
--master-machine-type n1-standard-4 --master-boot-disk-size 1000 \
--num-masters 1 \
--num-workers 5 --worker-machine-type n1-standard-4 \
--worker-boot-disk-size 1000 --image-version 1.5-debian \
--scopes https://www.googleapis.com/auth/cloud-platform \
--project $PROJECT_ID 

# --- load source data

bq --location=US mk $DATASET

bq load \
    --autodetect \
    --source_format=NEWLINE_DELIMITED_JSON \
    ${DATASET}.${TABLE} \
    gs://$GCS_BUCKET_NAME/${TABLE}/*

sleep 30 

# --- CSV

bq extract --location=US \
--destination_format CSV \
--compression GZIP \
--field_delimiter 'Tab' \
--print_header=false \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/CSV_GZIP/*.csv.gz

bq extract --location=US \
--destination_format CSV \
--field_delimiter 'Tab' \
--print_header=false \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/CSV/*.csv

# --- AVRO

bq extract --location=US \
--destination_format AVRO \
--compression SNAPPY \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/AVRO_SNAPPY/*.snappy.avro

bq extract --location=US \
--destination_format AVRO \
--compression DEFLATE \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/AVRO_DEFLATE/*.avro

bq extract --location=US \
--destination_format AVRO \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/AVRO/*.avro

# --- PARQUET

bq extract --location=US \
--destination_format PARQUET \
--compression SNAPPY \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/PARQUET_SNAPPY/*.snappy.parquet

bq extract --location=US \
--destination_format PARQUET \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/PARQUET/*.parquet

bq extract --location=US \
--destination_format PARQUET \
--compression GZIP \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/PARQUET_GZIP/*.parquet.gz

# --- JSON

bq extract --location=US \
--destination_format NEWLINE_DELIMITED_JSON \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/JSON/*.json

bq extract --location=US \
--destination_format NEWLINE_DELIMITED_JSON \
--compression GZIP \
$PROJECT_ID:$DATASET.$TABLE \
gs://$GCS_BUCKET_NAME/${TABLE}_format_testing/JSON_GZIP/*.json.gz


# edit the table-create.hql script
cp scritps/table-create.hql.template scripts/table-create.hql
sed -i "s|%%PROJECT_ID%%|$PROJECT_ID|g" scripts/table-create.hql
sed -i "s|%%TABLE%%|$TABLE|g" scripts/table-create.hql

gcloud dataproc jobs submit hive --region=$REGION --cluster=$CLUSTER_NAME --file=scripts/table-create.hql 
