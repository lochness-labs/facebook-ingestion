for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)   
    export "$KEY"=$VALUE
done

for file in "$LOCAL_FILE_PATH"/*; do
  aws s3 cp "$file" s3://"$BUCKET"/"$PREFIX" --profile "$PROFILE_NAME"
done