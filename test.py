import os
import io
import json
import os
import boto3
import time
import pytesseract
import requests
import threading
from PIL import Image
from pdf2image import convert_from_path, convert_from_bytes

MAX_TEXTRACT_THREADS = 20
SCALE_PERCENT = 60
s3 = boto3.client('s3', aws_access_key_id="AKIAQ5SD24NXSKX5624M", aws_secret_access_key="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")
s3_resourse = boto3.resource('s3', aws_access_key_id="AKIAQ5SD24NXSKX5624M", aws_secret_access_key="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")
response_thread = []
TEMP_IMG_LOCATION = "temp/"


def to_jpeg(img):
    # Convert PIL Image to JPEG
    buffer = io.BytesIO()
    img.save(buffer, "JPEG")
    buffer.seek(0)  # rewind pointer back to start
    return buffer


class UploadImgFilesThread(threading.Thread):
    def __init__(self, name, img):
        threading.Thread.__init__(self)
        self.name = name
        self.img = img
        self.bucket = "temporary-bucket-ocr"
        self.s3_resourse = boto3.resource('s3', aws_access_key_id="AKIAQ5SD24NXSKX5624M", aws_secret_access_key="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")
        
    def run(self):
        global response_thread
        upload_start_time = time.time()
        img_loc = TEMP_IMG_LOCATION + str(time.time()) + ".jpg"
        self.s3_resource.Object(self.bucket, img_loc).put(Body=to_jpeg(self.img).read())
        upload_end_time = time.time()
        response_thread.append(f"s3://{self.bucket}/{img_loc}")


class MyThreadTesseractOCR(threading.Thread):
    def __init__(self, name, img):
        threading.Thread.__init__(self)
        self.name = name
        self.img = img
    
    def run(self):
        global response_thread
        _r = requests.post('http://192.168.157.93:5000/extract', json={"key": self.img})
        response_thread.append(json.loads(_r.text))


def convert_pdf_to_img(obj_read):
    # Convert PDF to Image
    pages = convert_from_bytes(obj_read, 500,
                               # poppler_path=POPPLER_PATH, # Uncomment line in Windows, Comment line in Linux
                               fmt='jpg')
    return pages


def upload_pdf_images_to_s3(imgs):
    threads = []
    for page_no, img in enumerate(imgs[:MAX_TEXTRACT_THREADS]):
        threads.append(UploadImgFilesThread(str(page_no), img))
    for thread_obj in threads:
        thread_obj.start()
    for thread_obj in threads:
        thread_obj.join()
    return response_thread


def get_tesseract_ocr_response_threading(imgs):
    threads = []
    for page_no, img in enumerate(imgs[:MAX_TEXTRACT_THREADS]):
        threads.append(MyThreadTesseractOCR(str(page_no), img))
    for thread_obj in threads:
        thread_obj.start()
    for thread_obj in threads:
        thread_obj.join()
    return response_thread

files = [
    "s3://temporary-bucket-ocr/166429-00-1372 Everett Cash Mu Prop UY18 Dec-Dec18 SCOR Reinsuranc (1).pdf",
    "s3://temporary-bucket-ocr/166429-00-1372 Everett Cash Mu Prop UY18 Dec-Dec18 SCOR Reinsuranc.pdf",
]
out = []
out_time = []
s3 = boto3.client('s3', aws_access_key_id="AKIAQ5SD24NXSKX5624M", aws_secret_access_key="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")
for item in files:
    t1 = time.time()
    s3_object = s3.get_object(Bucket="poc-lamda-layer", Key="page4-converted.jpg")['Body'].read()
    imgs = convert_pdf_to_img(s3_object)
    locationList = upload_pdf_images_to_s3(imgs)
    tesseract_response_list = get_tesseract_ocr_response_threading(locationList)
    out.append(tesseract_response_list)
    t2 = time.time()
    out_time.append({"file": item, "time": t2-t1})
print(out)