#!/usr/bin/env python
import os, sys
import math
import boto
import argparse
import hashlib

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bucket", type=str, required=True, help="Selects the S3 bucket to upload data to")
    parser.add_argument("-f", "--file_path", type=str, required=True, help="Path of the file to be uploaded")
    parser.add_argument("-k", "--key", type=str, default=None, help="Key of the object. Same as file_path is undefined")
    parser.add_argument("-m","--mode", default='auto', choices=['auto', 'sync', 'simple-upload'],help="Method of upload")
    parser.add_argument("--chunk_size", default=5, type=int, help="Size of chunk in multi-part upload in MB")
    parser.add_argument("--multipart_threshold", default=10, type=int, help="Minimum size in MB to upload using multipart")
    args = parser.parse_args()

    if args.key is None:
        args.key = args.file_path
    args.chunk_size = args.chunk_size * 1024 *1024
    args.multipart_threshold = args.multipart_threshold * 1024 *1024
    return args

def multipart_upload_to_be_used(file_path):
    file_size = os.stat(file_path).st_size
    return file_size > args.multipart_threshold

def need_to_update(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    key = bucket.get_key(s3_path)
    if key is None:
        return True
    else:
        local_md5 = hashlib.md5(open(file_path, "rb").read()).hexdigest()
        return local_md5 != key.etag.strip('"')

def simple_upload(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    key = boto.s3.key.Key(bucket, s3_path)
    try:
        key.set_contents_from_filename(file_path)
        print "Upload completed successfully"
    except Exception as e:
        print "Upload failed"
        print e.message

def multipart_upload(s3, bucketname, file_path, s3_path, chunk_size):
    bucket = s3.get_bucket(bucketname)
    multipart_upload_request = bucket.initiate_multipart_upload(s3_path)

    file_size = os.stat(file_path).st_size
    chunks_count = int(math.ceil(file_size / float(chunk_size)))

    for i in range(chunks_count):
        offset = i * chunk_size
        remaining_bytes = file_size - offset
        payload_bytes = min([chunk_size, remaining_bytes])
        part_num = i + 1

        print "Uploading %d/%d" % (part_num, chunks_count)

        with open(file_path, 'r') as file_pointer:
            file_pointer.seek(offset)
            try:
                multipart_upload_request.upload_part_from_file(fp=file_pointer, part_num=part_num, size=payload_bytes)
                print ""
            except Exception as e:
                multipart_upload_request.cancel_upload()
                print "Upload failed"
                print e.message


    if len(multipart_upload_request.get_all_parts()) == chunks_count:
        multipart_upload_request.complete_upload()
        print "Upload completed successfully"
    else:
        multipart_upload_request.cancel_upload()
        print "Upload failed"

def upload(s3_connection, bucketname, file_path, s3_path, mode):
    if multipart_upload_to_be_used(file_path) and mode != 'simple-upload':
        multipart_upload(s3_connection, bucketname, file_path, s3_path)
    else:
        simple_upload(s3_connection, bucketname, file_path, s3_path)

def sync_to_s3():
    args = parse_arguments()
    s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    if args.mode != 'sync' or need_to_update(s3_connection, args.bucket, args.file_path, args.key):
        upload(s3_connection, args.bucket, args.file_path, args.key, args.mode)
    else:
        print "Nothing to update"


