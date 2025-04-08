import boto3
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from logging.handlers import RotatingFileHandler
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import logging
from botocore.config import Config
from enum import Enum
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

log_file = 'perlembagaan.log'
max_bytes = 1024 * 1024  # 1 MB
backup_count = 5  # Keep 5 backup files

file_handler = RotatingFileHandler(
    log_file,
    maxBytes=max_bytes,
    backupCount=backup_count
)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

import urllib.parse
import time

from typing import List, Dict
from botocore.exceptions import ClientError
import json
# SQLAlchemy setup
Base = declarative_base()

AWS_REGION = 'ap-southeast-1'


class PartyName(Base):
    __tablename__ = 'party_name'

    id = Column(Integer, primary_key=True)
    s3_party_name = Column(String(150),nullable=False)
    party_name = Column(String(150),nullable=True)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)

class ExtractedPerlembagaanTable(Base):
    __tablename__ = 'extracted_perlembagaan_1'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(500))
    party_id = Column(Integer, nullable=True)
    filename = Column(String(255))
    year_directory = Column(String(50))
    nama_pertubuhan = Column(String(150))
    perlembagaan = Column(Text)
    party_name = Column(String(150), nullable=True)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)

class ProcessingErrorType(Enum):
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    MISSING_MANDATORY_FIELD = "MISSING_MANDATORY_FIELD"
    INVALID_FORMAT = "INVALID_FORMAT"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    HEADER_MISMATCH = "HEADER_MISMATCH"
    EMPTY_TABLE = "EMPTY_TABLE"


class SijilType(Enum):
    BENDERA = "BENDERA"
    UNDANG = "UNDANG"
    ALAMAT = "ALAMAT"
    PENDAFTARAN = "PENDAFTARAN"
    NAMA = "NAMA",
    PERLEMBAGAAN = "PERLEMBAGAAN"

class ProcessedFile(Base):
    __tablename__ = 'processed_files'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(500), unique=True, nullable=False)
    filename = Column(String(255), nullable=False)
    year_directory = Column(String(50), nullable=False)
    processing_date = Column(DateTime, default=datetime.now)
    is_processed = Column(Boolean, default=False)
    total_pages = Column(Integer, nullable=True)
    processed_pages = Column(Integer, default=0)
    last_processed_page = Column(Integer, default=0)
    sijil_type = Column(String(50), nullable=True)
    error_message = Column(Text)

class UnprocessedData(Base):
    __tablename__ = 'unprocessed'

    id = Column(Integer, primary_key=True)
    s3_bucket = Column(String(255), nullable=False)
    s3_key = Column(String(500), nullable=False)
    page_number = Column(Integer, nullable=True)
    row_number = Column(Integer, nullable=True)
    error_type = Column(String(50), nullable=False)  # Stores ProcessingErrorType
    error_message = Column(String(1000), nullable=True)
    error_details = Column(JSON, nullable=True)  # Stores additional error context
    confidence_score = Column(Float, nullable=True)
    raw_text = Column(String(1000), nullable=True)  # Original text that failed
    processed_at = Column(DateTime, default=datetime.utcnow)
    is_reviewed = Column(Boolean, default=False)
    reviewed_by = Column(String(100), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    resolution_notes = Column(String(1000), nullable=True)
    sijil_type = Column(String(50), nullable=True)
    retry_count = Column(Integer, default=0)

    def log_error(self, session, error_type: ProcessingErrorType, error_context: dict):
        """
        Log error with detailed context
        """
        self.error_type = error_type.value
        self.error_details = json.dumps(error_context)
        session.add(self)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to log error: {str(e)}")
            raise

class S3DirectoryNavigator:
    def __init__(self, bucket_name: str):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name

    def list_directories(self, prefix: str) -> List[str]:
        """List all directories under the given prefix"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            directories = set()

            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/'):
                if 'CommonPrefixes' in page:
                    for prefix_obj in page['CommonPrefixes']:
                        dir_name = prefix_obj['Prefix'].rstrip('/').split('/')[-1]
                        directories.add(dir_name)

            return sorted(list(directories))
        except ClientError as e:
            print(f"Error listing directories: {e}")
            return []

    def list_pdfs(self, prefix: str) -> List[str]:
        """List all PDF files in the given prefix"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pdf_files = []

            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].lower().endswith('.pdf'):
                            pdf_name = obj['Key'].split('/')[-1]
                            pdf_files.append(pdf_name)

            return sorted(pdf_files)
        except ClientError as e:
            print(f"Error listing PDFs: {e}")
            return []

class PartyDirectoryProcessor:
    def __init__(self, bucket_name: str, root_directory: str, db_connection_string: str):
        self.navigator = S3DirectoryNavigator(bucket_name)
        self.root_directory = root_directory.rstrip('/')
        self.bucket_name = bucket_name
        self.region_name = 'ap-southeast-1'
        self.engine = create_engine(db_connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def process_perlembagaan(self, party_dir: str) -> Dict:
        """Process Perlembagaan directory for a party"""
        base_path = f"{self.root_directory}/{party_dir}/Induk/Perlembagaan"
        result = {
            'directory_name': 'Perlembagaan',
            'subdirectories': {}
        }

        # List directories in Perlembagaan
        directories = self.navigator.list_directories(f"{base_path}/")

        # Process each directory
        for directory in directories:
            pdfs = self.navigator.list_pdfs(f"{base_path}/{directory}/")
            result['subdirectories'][directory] = pdfs

        return result

    def process_sijil_specific_party(self, party_dir: str, part_info:PartyName) :
        """Process Sijil directory for a party"""
        #base_path = f"{self.root_directory}/{party_dir}/Induk/SIJIL" #to use this
        base_path = f"{self.root_directory}/{party_dir}/Induk/Perlembagaan"
        result = {
            'directory_name': 'Perlembagaan',
            'subdirectories': {}
        }

        self.process_years(base_path,party_dir,part_info) # process year

    def query_party_id(self, s3_party_name: str):
        try:
            # Query the database for the party name
            result = self.session.query(PartyName).filter_by(s3_party_name=s3_party_name).first()

            if result:
                # If a result is found, return the party name
                return result
            else:
                # If no result is found, return None or handle accordingly
                return None

        except Exception as e:
            # Handle any exceptions that occur during the query
            print(f"An error occurred: {e}")
            return None

    def process_parties(self, base_path:str):
        parties_directories = self.navigator.list_directories(f"{base_path}/")  # ['parties']

        # Process each directory
        for party in parties_directories:  # process every year
            print(f'party {party}')
            party_info = self.query_party_id(party)
            self.process_sijil_specific_party(party,party_info)
            #break

    def process_years(self, base_path:str,party_dir:str,party_info:PartyName):
        directories = self.navigator.list_directories(f"{base_path}/") #['years']

        # Process each directory
        for year in directories: #process every year
            self.process_perlembagaan_dir(base_path, year,party_dir, party_info) #check for bendera directory

    def process_perlembagaan_dir(self,base_path:str, year:str,party_dir:str, party_info:PartyName):
        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/")  # ['years']


        for perlembagaan_type_pdf in pdfs:
            self.process_perlembagaan_file(base_path,year,perlembagaan_type_pdf,party_dir, party_info) #process bendera type
            logger.info(f"Processed {perlembagaan_type_pdf}")





    def process_perlembagaan_file(self, base_path:str, year:str, perlembagaan_type_pdf:str,party_dir:str,party_info:PartyName):
        try:
        # Initialize clients
            path = f"{base_path}/{year}/{perlembagaan_type_pdf}"
            textract = boto3.client('textract', region_name= self.region_name)



            s3 = boto3.client('s3', region_name=AWS_REGION)

            print("Starting document analysis...")

            existing_file = self.session.query(ProcessedFile).filter_by(
                s3_path=path
            ).first()

            if existing_file and existing_file.is_processed:
                logger.info(f"Skipping already processed file: {path}")
                return False

            # Start Textract job
            response = textract.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': path

                    }
                },
                FeatureTypes=['LAYOUT']
            )

            job_id = response['JobId']
            print(f"Started Textract job with ID: {job_id}")

            # Wait for Textract job to complete
            print("Waiting for analysis to complete...")
            while True:
                response = textract.get_document_analysis(JobId=job_id)
                status = response['JobStatus']
                print(f"Current status: {status}")

                if status in ['SUCCEEDED', 'FAILED']:
                    break

                time.sleep(5)  # Wait 5 seconds before checking again

            if status == 'FAILED':
                raise Exception("Textract analysis failed")

            # Get all pages
            print("Extracting text from all pages...")
            pages = []
            next_token = None

            while True:
                if next_token:
                    response = textract.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                else:
                    response = textract.get_document_analysis(JobId=job_id)

                pages.extend(response['Blocks'])

                if 'NextToken' not in response:
                    break

                next_token = response['NextToken']

            # Extract text while preserving layout
            print("Processing text layout...")
            extracted_text = ""
            current_line = ""
            last_y = None

            for block in pages:
                if block['BlockType'] == 'LINE':
                    if last_y and abs(block['Geometry']['BoundingBox']['Top'] - last_y) > 0.015:
                        extracted_text += current_line.strip() + "\n"
                        current_line = ""

                    current_line += block['Text'] + " "
                    last_y = block['Geometry']['BoundingBox']['Top']

            if current_line:
                extracted_text += current_line.strip()

            #output_file_1 = 'perlembagaan_type_pdf_' + year + "_.txt"
            output_file_1 = perlembagaan_type_pdf + ".txt"
            with open(output_file_1, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            print(f"\nOutput saved to {output_file_1}")


            extractedPerlembagaan = ExtractedPerlembagaanTable(
                s3_path=path,
                filename=perlembagaan_type_pdf,
                perlembagaan=extracted_text,
                year_directory=year,
                nama_pertubuhan=party_dir,
                party_name=party_dir
            )
            if party_info:
                extractedPerlembagaan.party_id = party_info.id

            self.session.add(extractedPerlembagaan)

            processed_file = existing_file or ProcessedFile(
                s3_path=path,
                filename=perlembagaan_type_pdf,
                year_directory=year,
                sijil_type=SijilType.PERLEMBAGAAN.value
            )

            processed_file.is_processed = True
            self.session.add(processed_file)

            self.session.commit()

            logger.info(f"OK")

            return

            print("Using Bedrock to format text...")
            # Use Claude to format the text
            prompt = f"""
                Please format and structure the following extracted text while preserving all formatting, 
                paragraphs, and layout. Make sure to maintain the original document structure:
    
                {extracted_text}
                """

            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "temperature": 1,
                "top_p": 0.999,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            })

            # Invoke Claude model
            response = bedrock_runtime.invoke_model(
                modelId='apac.anthropic.claude-3-5-sonnet-20241022-v2:0',
                body=body,
                accept='application/json',
                contentType='application/json'
            )

            response_body = json.loads(response.get('body').read())
            formatted_text = response_body.get('content')[0].get('text')

            return formatted_text
        except Exception as e:
            self.session.rollback()
            unprocessed = UnprocessedData(
                s3_bucket=self.bucket_name,
                s3_key=path,
                page_number=1,
                error_message=ProcessingErrorType.EXTRACTION_FAILED.value,
                sijil_type=SijilType.PERLEMBAGAAN.value
            )
            error_context = {
                "error_message": str(e),
            }
            self.saved_to_unprocess_data(unprocessed, error_context)
            logger.error(f"Error in main process: {str(e)}")
            # raise

    def saved_to_unprocess_data(self, unprocessed: UnprocessedData, error_context: dict):
        unprocessed.log_error(self.session, ProcessingErrorType.LOW_CONFIDENCE, error_context)

    def process_all_parties(self) -> Dict:
        """Process all party directories"""
        result = {}

        try:
            # List all party directories
            party_directories = self.navigator.list_directories(f"{self.root_directory}/")

            # Process each party directory
            for party_dir in party_directories:
                party_result = {
                    'party_name': party_dir,
                    'perlembagaan': self.process_perlembagaan(party_dir),
                    'sijil': self.process_sijil(party_dir),
                    'ajk': self.process_ajk(party_dir)
                }
                result[party_dir] = party_result

            return result
        except Exception as e:
            print(f"Error processing parties: {e}")
            return {}


def main():
    #ori db_connection = "mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev"
    db_connection = "mysql+mysqlconnector://admin:Eroses123@rds-erosesrdsinstance-bl01iw3u4yka.c1q0w0yu2dv3.ap-southeast-5.rds.amazonaws.com:3306/eroses_migration"
    '''bucket_name = 'digitization-migration'
    root_directory = 'Parti_Politik-Induk'''
    bucket_name = 'induk-account-training'
    root_directory = 'Parti_Politik-Induk_Modified'

    processor = PartyDirectoryProcessor(bucket_name, root_directory,db_connection)
    processor.process_parties(root_directory)
    #result = processor.process_all_parties()
    #print(result)
    #processor.process_sijil_specific_party("AMANAH")
    #processor.process_sijil_specific_party("BARISAN KEMAJUAN INDIA SE-MALAYSIA")
    # Save result to JSON file
    import json

if __name__ == "__main__":
    main()
