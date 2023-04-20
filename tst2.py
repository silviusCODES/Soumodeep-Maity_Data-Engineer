import logging
import os
import requests
import boto3
import xml.etree.ElementTree as ET
from io import BytesIO
from zipfile import ZipFile

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def download_file(url):
    """
    step 1
    """
    response = requests.get(url)
    return response.content

def parse_xml(xml_content):
    """
    step 2
    """
    root = ET.fromstring(xml_content)
    for attachment in root.iter('Att'):
        file_type = attachment.find('AttTp').text
        if file_type == 'DLTINS':
            download_url = attachment.find('AttchmntUrl').text
            return download_url
    raise Exception('No attachment with file_type DLTINS found')

def download_zip(download_url):
    """
    Downloads the ZIP file from the specified URL and returns its contents.
    """
    zip_content = download_file(download_url)
    return ZipFile(BytesIO(zip_content))

def extract_xml(zip_file):
    """
    step 3
    """
    for filename in zip_file.namelist():
        if filename.endswith('.xml'):
            return zip_file.read(filename)
    raise Exception('No XML file found in the ZIP file')

def convert_to_csv(xml_content, header):
    """
    step 4
    """
    root = ET.fromstring(xml_content)
    rows = []
    for instrument in root.iter('FinInstrmGnlAttrbts'):
        row = {
            'FinInstrmGnlAttrbts.Id': instrument.find('Id').text,
            'FinInstrmGnlAttrbts.FullNm': instrument.find('FullNm').text,
            'FinInstrmGnlAttrbts.ClssfctnTp': instrument.find('ClssfctnTp').text,
            'FinInstrmGnlAttrbts.CmmdtyDerivInd': instrument.find('CmmdtyDerivInd').text,
            'FinInstrmGnlAttrbts.NtnlCcy': instrument.find('NtnlCcy').text,
            'Issr': instrument.find('Issr').text
        }
        rows.append(row)
    csv_content = header + '\n'
    for row in rows:
        csv_row = ','.join([row.get(key, '') for key in header.split(',')])
        csv_content += csv_row + '\n'
    return csv_content

def upload_to_s3(bucket, key, content):
    """
   step 5
    """
    try:
        s3 = boto3.resource('s3')
        s3.Object(bucket, key).put(Body=content)
        logging.info(f'Successfully uploaded file {key} to S3 bucket {bucket}.')
    except Exception as e:
        logging.error(f'Error uploading file {key} to S3 bucket {bucket}: {str(e)}')
        raise



