import time
import re
import io
import boto3
import json
import pytesseract
from flask import request
from app import app
from botocore.exceptions import ClientError
from PIL import Image

SCALE_PERCENT = 50
# pytesseract.pytesseract.tesseract_cmd = r'D:\\Users\\aanil\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract'


def get_bucket_and_object(uri):
    """
    Get S3 bucket name and object url from the input s3 uri
    """
    try:
        match = re.match(r's3:\/\/(.+?)\/(.+)', uri)
        return match.group(1), match.group(2)
    except Exception:
        raise Exception('Input Url Parsing Exception')


def get_s3_object(s3_client, bucket, key):
    try:
        return s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            raise ex
        else:
            raise Exception(f'AWSConnector : get_file_obj_from_s3 : {ex}')
    except Exception as e:
        raise Exception(f'AWSConnector : get_file_obj_from_s3 : {e}')


def image_preprocessing(img):
    """Preprocess the image for better performance"""
    width = int(img.size[1] * SCALE_PERCENT / 100)
    height = int(img.size[0] * SCALE_PERCENT / 100)
    dim = (width, height)
    # convert image to black and white
    img = img.convert('L')
    bw = img.point(lambda x: 0 if x < 128 else 255, '1')
    img_out = bw.resize(dim)
    return img_out


@app.route('/extract', methods=['POST'])
def extract():
    if request.method == "POST":
        s3_client = boto3.client('s3', aws_access_key_id="AKIAQ5SD24NXSKX5624M", aws_secret_access_key="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")
        t1 = time.time()
        bucket, key = get_bucket_and_object(json.loads(request.data.decode('utf8')).get("inputFormLocation"))
        s3_object = get_s3_object(s3_client, bucket, key)
        t2 = time.time()
        img = Image.open(io.BytesIO(s3_object))
        processed_img = image_preprocessing(img)
        t3 = time.time()

        text = pytesseract.image_to_string(processed_img)
        t4 =time.time()
        return {
            # "text": text,
            "s3_object_read_time": t2-t1,
            "preprocessing_time": t3-t2,
            "tesseract_time": t4-t3,
            "total_time": t4-t1
        }