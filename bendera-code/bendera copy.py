import boto3
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, JSON,Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import logging
from botocore.config import Config
from enum import Enum

import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import urllib.parse
import time
import json

from typing import List, Dict
from botocore.exceptions import ClientError
import json
# SQLAlchemy setup
#Base = declarative_base()




# SQLAlchemy setup
Base = declarative_base()
class ExtractedBenderaTable(Base):
    __tablename__ = 'extracted_bendera'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(500))
    filename = Column(String(255))
    year_directory = Column(String(50))
    nama_pertubuhan = Column(String(150))
    alamat_berdaftar = Column(String(1000))
    tarikh_berkuatkuasa = Column(String(50))
    nombor_ppm = Column(String(50))
    nombor_pendaftaran = Column(String(50))
    created_date = datetime.now()
    modified_date = datetime.now()

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
    NAMA = "NAMA"


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


class TextractCustomQueriesAdapter:
    def __init__(self, region_name: str = 'ap-southeast-1'):
        self.textract_client = boto3.client('textract', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)


    def analyze_document_with_adapter(
            self,
            bucket: str,
            document_key: str,
            adapter_id: str) -> Dict:
        """
        Analyze document using custom queries adapter
        """
        try:
            # Start asynchronous analysis job
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': document_key
                    }
                },
                FeatureTypes=['QUERIES'],
                QueriesConfig={
                    'Queries': [
                        {'Text': "What is nama pertubuhan"},
                        {'Text': "What is alamat berdaftar"},
                        {'Text': "what is the Tarikh Berkuatkuasa"},
                        {'Text': "What is nombor pendaftaran"},
                        {'Text': "what is the Nombor PPM"}
                    ]
                },
                AdaptersConfig={
                    'Adapters': [{
                        'AdapterId': adapter_id,  # The adapter ID needs to be in a dictionary,
                        'Version':'1'
                    }]
                }
            )

            job_id = response['JobId']
            logger.info(f"Started analysis job {job_id} for document {document_key}")

            return self._process_analysis_job(job_id)

        except ClientError as e:
            logger.error(f"AWS error during analysis: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            raise

    def _process_analysis_job(self, job_id: str, timeout: int = 900) -> Dict:
        """
        Process and monitor analysis job
        """
        try:
            # Wait for job completion
            if not self._wait_for_completion(job_id, timeout):
                raise Exception("Document analysis failed or timed out")

            # Get and process results
            return self._get_analysis_results(job_id)

        except Exception as e:
            logger.error(f"Error processing job {job_id}: {str(e)}")
            raise

    def _wait_for_completion(self, job_id: str, timeout: int) -> bool:
        """
        Wait for job completion with timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']

            if status == 'SUCCEEDED':
                logger.info(f"Job {job_id} completed successfully")
                return True
            elif status == 'FAILED':
                error_message = response.get('StatusMessage', 'No error message provided')
                logger.error(f"Job {job_id} failed: {error_message}")
                return False

            logger.info(f"Job {job_id} in progress... Status: {status}")
            time.sleep(5)

        logger.error(f"Job {job_id} timed out after {timeout} seconds")
        return False

    def _get_analysis_results(self, job_id: str) -> Dict:
        """
        Get and process analysis results
        """
        try:
            results = {}
            next_token = None

            while True:
                # Get results with pagination
                if next_token:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                else:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id
                    )

                # Process blocks
                self._process_blocks(response['Blocks'], results)

                # Check for more pages
                next_token = response.get('NextToken')
                if not next_token:
                    break

            return results

        except Exception as e:
            logger.error(f"Error getting results for job {job_id}: {str(e)}")
            raise

    def _process_blocks(self, blocks: List[Dict], results: Dict):
        """
        Process blocks from Textract response
        """
        query_mapping = {
            'what is the Tarikh Berkuatkuasa': 'organization_date',
            'What is nama pertubuhan': 'organization_name',
            "what is the Nombor PPM": 'ppm_no',
            "What is nombor pendaftaran": 'organization_no',
            "What is alamat berdaftar": 'organization_address',
        }

        for block in blocks:
            if block['BlockType'] == 'QUERY':
                query_text = block['Query']['Text']
                field_name = query_mapping.get(query_text, query_text)

                # Get answers for this query
                if 'Relationships' in block:
                    for rel in block['Relationships']:
                        if rel['Type'] == 'ANSWER':
                            answers = []
                            for answer_id in rel['Ids']:
                                # Find answer block
                                answer_block = next(
                                    (b for b in blocks if b['Id'] == answer_id),
                                    None
                                )
                                if answer_block:
                                    answers.append(answer_block['Text'])

                            # Store result
                            if answers:
                                results[field_name] = answers[0] if len(answers) == 1 else answers


'''
class BenderaExtractedInfo(Base):
    __tablename__ = 'bendera_extracted_info'

    id = Column(Integer, primary_key=True)
    nama_pertubuhan = Column(String)
    alamat_berdaftar = Column(String)
    tarikh_berkuatkuasa = Column(String)
    nombor_ppm = Column(String)
    nombor_pendaftaran = Column(String)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)
'''

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
        self.engine = create_engine(db_connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.bendera_keys = {
            'organization_address',
            'organization_date',
            'ppm_no',
            'organization_name',
            'organization_no'
        }


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

    def process_sijil_specific_party(self, party_dir: str) :
        """Process Sijil directory for a party"""
        #base_path = f"{self.root_directory}/{party_dir}/Induk/SIJIL" #to use this
        base_path = f"{self.root_directory}/{party_dir}/Induk/Sijil"
        result = {
            'directory_name': 'Sijil',
            'subdirectories': {}
        }

        self.process_years(base_path) # process year

    def process_years(self, base_path:str):
        directories = self.navigator.list_directories(f"{base_path}/") #['years']

        # Process each directory
        for year in directories: #process every year
            self.process_sijil_dir(base_path, year) #check for bendera directory

    def process_parties(self, base_path:str):
        parties_directories = self.navigator.list_directories(f"{base_path}/")  # ['parties']

        # Process each directory
        for party in parties_directories:  # process every year
            print(f'party {party}')
            self.process_sijil_specific_party(party)
            #break

    def process_sijil_dir(self,base_path:str, year:str):
        directories = self.navigator.list_directories(f"{base_path}/{year}/")  # ['years']


        for sijil_type_dir in directories:
            if 'bendera' in sijil_type_dir.lower() :
                self.process_bendera_dir(base_path,year,sijil_type_dir) #process bendera type
                break
            else:
                continue





    def process_bendera_dir(self, base_path:str, year:str, sijil_name:str):

        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/{sijil_name}/")

        for pdf in pdfs:
            print(f"PDF: {pdf}")
            print(f"Path: {base_path}/{year}/{sijil_name}/{pdf}")
            self.process_bendera_pdf(f"{base_path}/{year}/{sijil_name}/{pdf}",base_path, year, pdf)

    def process_bendera_pdf(self,path:str,base_path:str, year:str, pdf:str):
        region_name = 'ap-southeast-1'
        analyzer = TextractCustomQueriesAdapter(region_name=region_name)
        adapter_id = '41fa70adba5b'  # Replace with your actual adapter ID
        #bucket_name = 'digitization-migration'
        bucket_name = 'induk-account-training'
        #path="Parti_Politik-Induk/AMANAH/Induk/SIJIL/2015/Kebenaran Menggunakan Bendera, Lambang, Lencana atau Tanda-Tanda Lain/Sijil Kebenaran Menggunakan Bendera Lambang Lencana atau Tanda Tanda Lain.pdf"
        try:

            existing_file = self.session.query(ProcessedFile).filter_by(
                s3_path=path
            ).first()

            if existing_file and existing_file.is_processed:
                logger.info(f"Skipping already processed file: {path}")
                return False
            # Analyze document
            results = analyzer.analyze_document_with_adapter(
                bucket_name,
                path,
                adapter_id
            )
            benderaData = ExtractedBenderaTable()
            key_value = {}
            print("\nExtracted Information:")
            for field, value in results.items():
                print(f"\n{field}:")
                if isinstance(value, list):
                    for item in value:
                        print(f"  - {item}")
                else:
                    print(f"  {value}")
                    key_value[field] = value

            # Save results

            '''
              id = Column(Integer, primary_key=True)
                s3_path = Column(String(500))
                filename = Column(String(255))
                year_directory = Column(String(50))
                nama_pertubuhan = Column(String(150))
                alamat_berdaftar = Column(String(1000))
                tarikh_berkuatkuasa = Column(String(50))
                nombor_ppm = Column(String(50))
                nombor_pendaftaran = Column(String(50))
                created_date = datetime.now()
                modified_date = datetime.now()
                
                
                
                 'what is the Tarikh Berkuatkuasa': 'organization_date',
            'what is the Nama Pertubuhan': 'organization_name',
            "what is the Nombor PPM": 'ppm_no',
            "what is the Nombor Pendaftaran": 'organization_no',
            "What is alamat berdaftar": 'organization_address',
            '''
            not_found_keys: List[str] = []
            for key in self.bendera_keys:
                print(f"Key: {key}")
                if not (key in key_value.keys()):
                    not_found_keys.append(key)

            if  len(not_found_keys) > 0:
                print(f"Keys not found: {not_found_keys}")
                unfound_keys = {}
                for unfound_key in not_found_keys:
                    unfound_keys[unfound_key] = 'Not Found'
                error_context = {
                    "extracted_data": json.dumps(key_value),
                    "unfound_keys" : json.dumps(unfound_keys),
                }
                unprocessed = UnprocessedData(
                    s3_bucket=bucket_name,
                    s3_key=path,
                    page_number=1,
                    error_message=ProcessingErrorType.MISSING_MANDATORY_FIELD.value.lower(),
                    sijil_type=SijilType.BENDERA.value

                )
                self.saved_to_unprocess_data(unprocessed, error_context)
                return False

            for key, value in key_value.items():
                if key == 'organization_name':
                    benderaData.nama_pertubuhan =  value
                elif key == 'organization_address':
                    benderaData.alamat_berdaftar = value
                elif key == 'organization_date':
                    benderaData.tarikh_berkuatkuasa = value
                elif key == 'ppm_no':
                    benderaData.nombor_ppm = value
                elif key == 'organization_no':
                    benderaData.nombor_pendaftaran = value
                elif key == 'organization_name':
                    benderaData.nama_pertubuhan = value
            benderaData.s3_path = path
            benderaData.year_directory = year
            benderaData.filename = pdf
            self.session.add(benderaData)

            processed_file = existing_file or ProcessedFile(
                s3_path=path,
                filename=pdf,
                year_directory=year,
                sijil_type=SijilType.BENDERA.value
            )

            processed_file.is_processed = True
            self.session.add(processed_file)

            self.session.commit()



            logger.info(f"OK")

        except Exception as e:
            self.session.rollback()
            unprocessed = UnprocessedData(
                s3_bucket=bucket_name,
                s3_key=path,
                page_number=1,
                error_message= ProcessingErrorType.EXTRACTION_FAILED.value,
                sijil_type = SijilType.BENDERA.value
            )
            error_context = {
                "error_message": str(e),
            }
            self.saved_to_unprocess_data(unprocessed, error_context)
            logger.error(f"Error in main process: {str(e)}")
            #ckf raise

    def saved_to_unprocess_data(self, unprocessed:UnprocessedData, error_context:str):
        unprocessed.log_error(self.session, ProcessingErrorType.LOW_CONFIDENCE, error_context)

    def process_sijil(self, party_dir: str) -> Dict:
        """Process Sijil directory for a party"""
        base_path = f"{self.root_directory}/{party_dir}/Induk/Sijil"
        result = {
            'directory_name': 'Sijil',
            'subdirectories': {}
        }

        # List directories in Sijil
        directories = self.navigator.list_directories(f"{base_path}/")

        # Process each directory
        for directory in directories:
            pdfs = self.navigator.list_pdfs(f"{base_path}/{directory}/")
            result['subdirectories'][directory] = pdfs

        return result

    def process_ajk(self, party_dir: str) -> Dict:
        """Process AJK directory for a party"""
        base_path = f"{self.root_directory}/{party_dir}/Induk/AJK"
        result = {
            'directory_name': 'AJK',
            'subdirectories': {}
        }

        # List directories in AJK
        directories = self.navigator.list_directories(f"{base_path}/")

        # Process each directory
        for directory in directories:
            pdfs = self.navigator.list_pdfs(f"{base_path}/{directory}/")
            result['subdirectories'][directory] = pdfs

        return result

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
    db_connection = "mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev"
    '''bucket_name = 'digitization-migration'
    root_directory = 'Parti_Politik-Induk'''
    bucket_name = 'induk-account-training'
    root_directory = 'Parti_Politik-Induk_Modified'
    processor = PartyDirectoryProcessor(bucket_name, root_directory,db_connection)
    processor.process_parties(root_directory)
    #result = processor.process_all_parties()
    #print(result)
    #ckf ori processor.process_sijil_specific_party("AMANAH")
    #processor.process_sijil_specific_party("BARISAN KEMAJUAN INDIA SE-MALAYSIA")

    # Save result to JSON file
    import json

if __name__ == "__main__":
    main()
