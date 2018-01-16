# s3-pairtree-copy

Simple script to copy select IIIF objects from one Pairtree S3 bucket to another.

How to use:

    AWS_PROFILE="sinai-data" java -jar s3-pairtree-copy-0.0.1.jar -c input.csv -o SlvNF-3;Grk-925 -s sinai-data -d test-sinai-data

To see what arguments mean:

    AWS_PROFILE="sinai-data" java -jar s3-pairtree-copy-0.0.1.jar -h