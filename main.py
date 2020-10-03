# Python modules
from flask import Flask
app = Flask(__name__)
app.config["DEBUG"] = True

from flask import request

from dotenv import load_dotenv
load_dotenv()

import time
import os
import json
import textract
import lib
import spacy
import boto3
import botocore
# import tika
# from tika import parser
import sys

# custom files
import connect
import word_parser
import field_extraction
import subprocess
from flask import jsonify

@app.route('/parse', methods=['POST'])
def main():
    
    if request.method == 'POST':
        project_secret_key = None
        if 'Authorization' in request.headers:
            project_secret_key = request.headers.get('Authorization')

        if project_secret_key != os.getenv( 'AUTH'):
            return 'Authorization is failed.'

        data = json.loads(request.data)

        # check data exists or not
        if 'key' not in data:
            return 'Data Is required'

        print ('------**************************------')
        
        KEY = data['key']
        BUCKET_NAME = os.getenv( 'BUCKET_NAME')
        extension = data['extension']
        local_file_path = os.getenv( 'TEMP_FILE_PATH')
        local_file = str(int(round(time.time() * 1000)))
        local_file_name = local_file_path+local_file+ "."+extension
        s3 = boto3.resource('s3',
         aws_access_key_id=os.getenv( 'AWS_ACCESS_KEY'),
         aws_secret_access_key= os.getenv( 'AWS_SECRET_KEY'))

        sections = {}
        asset_path = os.path.abspath(os.getenv( 'ABS_FILE_PATH'))
        nlp = spacy.load(asset_path)

        try:

            tempFileName = os.getenv( 'TEMP_FILE_PATH') + str(int(round(time.time() * 1000)))
            s3.Bucket(BUCKET_NAME).download_file(KEY, local_file_name)

            if extension=='pdf':
                # Code to convert PDF to docx========== START ==================
                import pdftotext
                from docx import Document
                from docx.shared import Inches
                document = Document()

                with open(local_file_name, "rb") as f:
                    pdf = pdftotext.PDF(f)
                 
                for page in pdf:
                    print(page)
                    document.add_paragraph(page)
                local_file_name = tempFileName + ".docx"
                document.save(local_file_name)
                # Code to convert PDF to docx========== END ==================

            if extension=='doc':
                
                subprocess.call(["lowriter", "--convert-to", "pdf", local_file_name])
                subprocess.call(["lowriter", "--convert-to", "docx", local_file_name])
                local_file_name = local_file_path+local_file+ ".docx"
                subprocess.call(["mv", local_file+".docx", local_file_name])

            if extension=='docx':
                subprocess.call(["lowriter", "--convert-to", "pdf", local_file_name])

            ## code to create PDFtoIMG =============== START ==============
            from pdf2image import convert_from_path
            images = ''
            if extension=='pdf':
                images = convert_from_path(local_file+ ".pdf")
            else:
                images = convert_from_path(local_file+ ".pdf")
            from pdf2image.exceptions import (
             PDFInfoNotInstalledError,
             PDFPageCountError,
             PDFSyntaxError
            )
            imgList = []
            for i, image in enumerate(images):
                i=i+1
                fname = local_file_path+local_file+'image'+str(i)+'.jpg'
                image.save(fname, "JPEG")

                ## code to upload IMG to S3 =============== START ==============
                s3.meta.client.upload_file(fname, BUCKET_NAME, local_file+'image'+str(i)+'.jpg', ExtraArgs={'ACL':'public-read'})
                imgList.append('https://'+BUCKET_NAME+'.s3.ap-south-1.amazonaws.com/'+local_file+'image'+str(i)+'.jpg')
                ## code to upload IMG to S3 =============== START ==============

            ## code to create PDFtoIMG =============== END ==============

            text = textract.process(local_file_name)
            resume_sections = word_parser.word_prarser(local_file_name)

            sections['email'] = field_extraction.extect_email(text)
            sections['phone'] = field_extraction.extract_phone(text)
            sections['name'] = field_extraction.extract_name(text)
            sections['imgList'] = imgList

            # Extract titles
            titles = field_extraction.extract_titles(resume_sections)
            # Extract mapping
            mapping = field_extraction.extract_mappings(titles)

            for each in field_extraction.TITLES_TO_MAP:
                # observations[each] = observations.apply(field_extraction.extract_mapping_section, axis=1)
                sections[each] = field_extraction.extract_mapping_section(resume_sections, mapping, each)

            # Remove file local_file_name
            if os.path.exists(local_file_name):
                os.remove(local_file_name)

            sections['imgList'] = imgList

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
        # print('---------------@@@@@@@@@@@@----------- sections', sections)
        return jsonify(sections)
    else:
        return 'Only POST Method is allowed!'
# print('------->>>>>', __name__)        
# if __name__ == 'main':
   # app.run(host='0.0.0.0', port='4999')    
app.run(host='0.0.0.0', port='4997')
