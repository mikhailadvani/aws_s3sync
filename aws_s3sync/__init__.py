#!/usr/bin/env python
import os
import math
import boto
import argparse
import hashlib
import time

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def log(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print "%s %s" % (timestamp, msg)

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

def multipart_upload_to_be_used(file_path, multipart_threshold):
    file_size = os.stat(file_path).st_size
    return file_size > multipart_threshold

def need_to_update(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    key = bucket.get_key(s3_path)
    log("key=%s" % key)
    if key is None:
        return True
    else:
        local_md5 = hashlib.md5(open(file_path, "rb").read()).hexdigest()
        log("local_signature=%s" % local_md5)
        log("remote_signature=%s" % key.etag.strip('"'))
        return local_md5 != key.etag.strip('"')

def simple_upload(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    key = boto.s3.key.Key(bucket, s3_path)
    try:
        key.set_contents_from_filename(file_path)
        log("Upload completed successfully")
    except Exception as e:
        log("Upload failed")
        log(e.message)

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

        log("Uploading %d/%d" % (part_num, chunks_count))

        with open(file_path, 'r') as file_pointer:
            file_pointer.seek(offset)
            try:
                multipart_upload_request.upload_part_from_file(fp=file_pointer, part_num=part_num, size=payload_bytes)
            except Exception as e:
                multipart_upload_request.cancel_upload()
                log("Upload failed")
                log(e.message)


    if len(multipart_upload_request.get_all_parts()) == chunks_count:
        multipart_upload_request.complete_upload()
        log("Upload completed successfully")
    else:
        multipart_upload_request.cancel_upload()
        log("Upload failed")

def upload(s3_connection, bucketname, file_path, s3_path, mode, chunk_size, multipart_threshold):
    if multipart_upload_to_be_used(file_path, multipart_threshold) and mode != 'simple-upload':
        log("payload_mode=multi_part")
        multipart_upload(s3_connection, bucketname, file_path, s3_path, chunk_size)
    else:
        log("payload_mode=single_part")
        simple_upload(s3_connection, bucketname, file_path, s3_path)

def sync_to_s3():
    args = parse_arguments()
    s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    log("s3_connection=established")
    upload_needed = need_to_update(s3_connection, args.bucket, args.file_path, args.key)
    log("upload_mode=%s" % args.mode)
    log("upload_needed=%s" % upload_needed)
    if args.mode != 'sync' or upload_needed:
        upload(s3_connection, args.bucket, args.file_path, args.key, args.mode, args.chunk_size, args.multipart_threshold)
    else:
        log("Nothing to update")


