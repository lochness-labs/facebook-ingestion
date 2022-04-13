# source s3_upload.sh PROFILE_NAME="test" LOCAL_FILE_PATH="./libraries" BUCKET="example-code-s3-bucket-name" PREFIX="glue-code/scripts/facebook_ingestion_dev/libraries/"

for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)   
    export "$KEY"=$VALUE
done

for file in "$LOCAL_FILE_PATH"/*; do
  aws s3 cp "$file" s3://"$BUCKET"/"$PREFIX" --profile "$PROFILE_NAME"
done