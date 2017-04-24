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
    parser.add_argument("-b", "--bucket", type=str, required=True, help="Upload: Selects the S3 bucket to upload data to. Download: Selects the S3 bucket to download data from")
    parser.add_argument("-f", "--file_path", type=str, required=True, help="Upload: Path of the file to be uploaded. Download: Path to download file to")
    parser.add_argument("-k", "--key", type=str, default=None, help="Key of the object. Same as file_path is undefined for upload")
    parser.add_argument("-m","--mode", default='auto', choices=['auto', 'sync', 'single-part-upload'],help="Mode of upload/download")
    parser.add_argument("--chunk_size", default=5, type=int, help="Size of chunk in multipart upload in MB. Minimum 5. Refer http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html")
    parser.add_argument("--multipart_threshold", default=10, type=int, help="Minimum size in MB to upload using multipart")
    args = parser.parse_args()

    if args.key is None:
        args.key = args.file_path
    if args.chunk_size < 5:
        log("Chunk size needs to be a minimum of 5 MB")
        exit(1)
    args.chunk_size = args.chunk_size * 1024 *1024
    args.multipart_threshold = args.multipart_threshold * 1024 *1024
    return args

def multipart_upload_to_be_used(file_path, multipart_threshold):
    file_size = os.stat(file_path).st_size
    log("event=get_file_size size=%d" % file_size)
    return file_size > multipart_threshold

def need_to_update(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    log("event=get_bucket bucket=%s" % bucket_name)
    key = bucket.get_key(s3_path)
    log("event=get_key key=%s" % key)
    if key is None:
        return True
    else:
        local_md5 = hashlib.md5(open(file_path, "rb").read()).hexdigest()
        log("event=generate_local_signature local_signature=%s" % local_md5)
        remote_md5 = key.get_metadata('md5')
        log("event=get_remote_signature remote_signature=%s" % remote_md5)
        return local_md5 != remote_md5

def need_to_fetch(s3_connection, bucket_name, file_path, s3_path):
    if not os.path.isfile(file_path):
        log("event=check_file_exists_locally status=failed")
        return True
    else:
        log("event=check_file_exists_locally status=success")
        return need_to_update(s3_connection, bucket_name, file_path, s3_path)

def simple_upload(s3_connection, bucket_name, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucket_name)
    key = boto.s3.key.Key(bucket, s3_path)
    key.set_metadata('md5', hashlib.md5(open(file_path, "rb").read()).hexdigest())
    try:
        key.set_contents_from_filename(file_path)
        log("event=upload_complete status=success")
    except Exception as e:
        log("event=upload_complete status=failed")
        log(str(e))

def multipart_upload(s3, bucketname, file_path, s3_path, chunk_size):
    log("event=get_bucket bucket=%s" % bucketname)
    bucket = s3.get_bucket(bucketname)
    log("event=multi_part_request_initiated bucket=%s status=triggered" % bucketname)
    multipart_upload_request = bucket.initiate_multipart_upload(s3_path, metadata={'md5': hashlib.md5(open(file_path, "rb").read()).hexdigest()})
    log("event=multi_part_request_initiated status=success")
    file_size = os.stat(file_path).st_size
    chunks_count = int(math.ceil(file_size / float(chunk_size)))

    for i in range(chunks_count):
        offset = i * chunk_size
        remaining_bytes = file_size - offset
        payload_bytes = min([chunk_size, remaining_bytes])
        part_num = i + 1

        log("event=upload_part part_num=%d total_parts=%d" % (part_num, chunks_count))

        with open(file_path, 'r') as file_pointer:
            file_pointer.seek(offset)
            upload_part(file_pointer, multipart_upload_request, part_num, payload_bytes)
    multipart_upload_request.complete_upload()
    log("event=upload_complete status=success")

def upload_part(file_pointer, multipart_upload_request, part_num, payload_bytes, attempt=1):
    if attempt > 5:
        log("event=upload_complete status=failed")
        multipart_upload_request.cancel_upload()

    try:
        multipart_upload_request.upload_part_from_file(fp=file_pointer, part_num=part_num, size=payload_bytes)
    except Exception as e:
        pause_between_retries = 30
        log("event=upload_part_failed part_num=%d attempt=%d retry_after=%d" % (part_num, attempt, pause_between_retries))
        log(str(e))
        time.sleep(pause_between_retries)
        upload_part(file_pointer, multipart_upload_request, part_num, payload_bytes, attempt + 1)


def upload(s3_connection, bucketname, file_path, s3_path, mode, chunk_size, multipart_threshold):
    if multipart_upload_to_be_used(file_path, multipart_threshold) and mode != 'single-part-upload':
        log("event=start_multipart_upload bucket=%s file_path=%s key=%s chunk_size=%d multipart_threshold=%d" % (bucketname, file_path, s3_path, chunk_size, multipart_threshold))
        multipart_upload(s3_connection, bucketname, file_path, s3_path, chunk_size)
    else:
        log("event=start_simple_upload bucket=%s file_path=%s key=%s" % (bucketname, file_path, s3_path))
        simple_upload(s3_connection, bucketname, file_path, s3_path)

def download(s3_connection, bucketname, file_path, s3_path):
    bucket = s3_connection.get_bucket(bucketname)
    key = boto.s3.key.Key(bucket, s3_path)
    try:
        log("event=start_download key=%s" % s3_path)
        key.get_contents_to_filename(file_path)
        log("event=download_complete status=success")
    except Exception as e:
        log("event=download_complete status=failed")
        log(str(e))

def sync_to_s3():
    args = parse_arguments()
    s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    log("event=s3_connection state=established")
    upload_needed = need_to_update(s3_connection, args.bucket, args.file_path, args.key)
    log("upload_mode=%s" % args.mode)
    log("upload_needed=%s" % upload_needed)
    if args.mode != 'sync' or upload_needed:
        log("event=choose_upload_type")
        upload(s3_connection, args.bucket, args.file_path, args.key, args.mode, args.chunk_size, args.multipart_threshold)
    else:
        log("event=upload_skipped")

def sync_from_s3():
    args = parse_arguments()
    s3_connection = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    log("event=s3_connection state=established")
    download_needed = need_to_fetch(s3_connection, args.bucket, args.file_path, args.key)
    log("download_mode=%s" % args.mode)
    log("download_needed=%s" % download_needed)
    if args.mode != 'sync' or download_needed:
        download(s3_connection, args.bucket, args.file_path, args.key)
    else:
        log("event=download_skipped")