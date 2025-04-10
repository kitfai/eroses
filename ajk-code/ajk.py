import boto3
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean,Boolean, Text, ForeignKey,JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fuzzywuzzy import fuzz
from logging.handlers import RotatingFileHandler
from fuzzywuzzy import process
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
import logging
from botocore.config import Config

from typing import Tuple, Dict, List
import difflib
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

log_file = 'ajk.log'
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
from enum import Enum

from typing import List, Dict
from botocore.exceptions import ClientError
import json
# SQLAlchemy setup
#Base = declarative_base()




# SQLAlchemy setup
Base = declarative_base()

'''
class ProcessedFile(Base):
    __tablename__ = 'processed_files'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(500), unique=True, nullable=False)
    filename = Column(String(255), nullable=False)
    year_directory = Column(String(50), nullable=False)
    processing_date = Column(DateTime, default=datetime.now)
    is_processed = Column(Boolean, default=False)
    total_pages = Column(Integer)
    processed_pages = Column(Integer, default=0)
    last_processed_page = Column(Integer, default=0)
    error_message = Column(Text)'''

class PartyName(Base):
    __tablename__ = 'party_name'

    id = Column(Integer, primary_key=True)
    s3_party_name = Column(String(150),nullable=False)
    party_name = Column(String(150),nullable=True)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)

class ExtractedTableData(Base):
    __tablename__ = 'extracted_table_data'

    id = Column(Integer, primary_key=True)
    #ckf file_id = Column(Integer, ForeignKey('processed_files.id'), nullable=False)
    page_number = Column(Integer, nullable=False)
    table_number = Column(Integer, nullable=False)
    row_number = Column(Integer, nullable=False)
    column_name = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    confidence_score = Column(Float, nullable=False)
    extraction_date = Column(DateTime, default=datetime.now),
    path = Column(String(500), nullable=False)


class UnprocessedCells(Base):
    __tablename__ = 'unprocessed_cells'

    id = Column(Integer, primary_key=True)
    #ckf file_id = Column(Integer, ForeignKey('processed_files.id'), nullable=False)
    page_number = Column(Integer, nullable=False)
    table_number = Column(Integer, nullable=False)
    row_number = Column(Integer, nullable=False)
    column_name = Column(String(255))
    content = Column(Text, nullable=False)  # Raw content
    cleaned_content = Column(Text)  # Cleaned content
    confidence_score = Column(Float, nullable=False)
    reason = Column(String(255))
    extraction_date = Column(DateTime, default=datetime.now),
    path = Column(String(500), nullable=False)



class AJK(Base):
    __tablename__ = 'extracted_ajk'

    id = Column(Integer, primary_key=True)
    jawatan = Column(String(100), nullable=False)
    nama = Column(String(255), nullable=False)
    no_kp = Column(String(100), nullable=True)
    no_telefon = Column(String(600), nullable=True)
    alamat = Column(String(500), nullable=True)
    majikan = Column(String(255), nullable=True)
    tahun = Column(String(50), nullable=True)
    pekerjaan = Column(String(500), nullable=True)
    jantina = Column(String(50), nullable=True)
    confidence_score = Column(Float, nullable=True)
    party_name = Column(String(150))
    party_id = Column(Integer,nullable=True, default=-1)
    path = Column(String(500), nullable=True)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)

class AJKExtractedData(Base):
    __tablename__ = 'ajk_test'

    id = Column(Integer, primary_key=True)
    jawatan = Column(String(100), nullable=False)
    nama = Column(String(255), nullable=False)
    no_kp = Column(String(100), nullable=True)
    no_telefon = Column(String(100), nullable=True)
    alamat = Column(String(500), nullable=True)
    majikan = Column(String(255), nullable=True)
    tahun = Column(String(4), nullable=True)
    pekerjaan = Column(String(255), nullable=True)
    jantina = Column(String(50), nullable=True)
    confidence_score = Column(Float, nullable=True)
    party_name = Column(String(150))
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
    AJK = "AJK"

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


class UnprocessedDataHandler:
    def __init__(self, session):
        self.session = session
        #self.confidence_threshold = 85.0  # Minimum confidence score required
        self.confidence_threshold = 73.0  # Minimum confidence score required

    def handle_low_confidence(self, s3_bucket: str, s3_key: str, row_data: dict,
                              confidence_score: float, page_number: int = None,
                              row_number: int = None) -> None:
        """
        Handle cases where extracted text has low confidence
        """
        if confidence_score < self.confidence_threshold:
            error_context = {
                "confidence_score": confidence_score,
                "threshold": self.confidence_threshold,
                "extracted_data": row_data
            }

            unprocessed = UnprocessedData(
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                page_number=page_number,
                row_number=row_number,
                confidence_score=confidence_score,
                raw_text=json.dumps(row_data)
            )
            unprocessed.log_error(self.session, ProcessingErrorType.LOW_CONFIDENCE, error_context)

    def handle_missing_mandatory_fields(self, s3_bucket: str, s3_key: str,
                                        row_data: dict, missing_fields: List[str],
                                        page_number: int = None,
                                        row_number: int = None) -> None:
        """
        Handle cases where mandatory fields are missing
        """
        error_context = {
            "missing_fields": missing_fields,
            "provided_data": row_data
        }

        unprocessed = UnprocessedData(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            page_number=page_number,
            row_number=row_number,
            raw_text=json.dumps(row_data)
        )
        unprocessed.log_error(self.session, ProcessingErrorType.MISSING_MANDATORY_FIELD, error_context)

    def handle_extraction_error(self, s3_bucket: str, s3_key: str, error: Exception,
                                page_number: int = None) -> None:
        """
        Handle general extraction errors
        """
        error_context = {
            "error_type": type(error).__name__,
            "error_message": str(error)
        }

        unprocessed = UnprocessedData(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            page_number=page_number
        )
        unprocessed.log_error(self.session, ProcessingErrorType.EXTRACTION_FAILED, error_context)

    def get_unreviewed_records(self) -> List[UnprocessedData]:
        """
        Retrieve all unreviewed records
        """
        return self.session.query(UnprocessedData) \
            .filter(UnprocessedData.is_reviewed == False) \
            .order_by(UnprocessedData.processed_at.desc()) \
            .all()

    def mark_as_reviewed(self, record_id: int, reviewer: str,
                         resolution_notes: str) -> None:
        """
        Mark a record as reviewed with resolution notes
        """
        record = self.session.query(UnprocessedData).get(record_id)
        if record:
            record.is_reviewed = True
            record.reviewed_by = reviewer
            record.reviewed_at = datetime.utcnow()
            record.resolution_notes = resolution_notes
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                logger.error(f"Failed to mark record as reviewed: {str(e)}")
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

class TextractTableProcessor:
    def __init__(self):
        self.standard_headers = {
            'jawatan': ['jawatan', 'position', 'pangkat', 'designation'],
            'nama': ['nama', 'name', 'full name', 'nama penuh'],
            'no_kp': ['no_kp', 'nokp', 'ic', 'no ic', 'kad pengenalan', 'ic number', "No. Kad Pengenalan", "no. kp", "no.kp","no. mykad", "no. kad pengenalan"],
            'no_telefon': ['no_telefon', 'telefon', 'phone', 'tel', 'no tel', 'phone number',"no. telefon", "no.tel"],
            'alamat': ['alamat', 'address', 'residential address'],
            'majikan': ['majikan', 'employer', 'company', 'syarikat'],
            'tahun': ['tahun', 'year', 'tahun semasa'],
            'jantina': ['jantina', 'gender', 'sex', 'jenis jantina',"l/p"],
            'pekerjaan': ['pekerjaan', 'job', 'occupation'],
        }
        #self.confidence_threshold = 85.0
        self.confidence_threshold = 73.0
        self.first_table_headers = {}


    def standardize_column_name_test(self, column_name: str) -> Tuple[str, float]:
        """
        Standardize column names using direct matching and fuzzy matching.
        Handles cases where column_name contains more words than variations.
        Prioritizes longer matches to avoid incorrect matches when multiple keywords are present.
        Returns (standardized_name, match_score)
        """
        if not column_name:
            return None, 0.0

        column_name = column_name.lower().strip()
        normalized_column_name = re.sub(r"[^a-zA-Z0-9\s]", "", column_name)  # Remove special characters

        # Direct match check (whole column name)
        for std_name, variations in self.standard_headers.items():
            if column_name in variations or normalized_column_name in variations:
                logger.info(f"Direct match found for '{column_name}' to '{std_name}'")
                return std_name, 100.0

        # Direct match check (variation is part of column name)
        best_partial_match = None
        best_partial_match_length = 0

        for std_name, variations in self.standard_headers.items():
            for variation in variations:
                normalized_variation = re.sub(r"[^a-zA-Z0-9\s]", "", variation.lower())

                if variation.lower() == column_name.lower():
                    return variation, 100.0

                if normalized_variation in normalized_column_name:
                    if len(normalized_variation) > best_partial_match_length:
                        best_partial_match_length = len(normalized_variation)
                        best_partial_match = (std_name, 100.0)
                        logger.info(
                            f"Partial match found for '{column_name}' to '{std_name}' (variation: '{variation}')")

                if normalized_column_name in normalized_variation:
                    if len(normalized_column_name) > best_partial_match_length:
                        best_partial_match_length = len(normalized_column_name)
                        best_partial_match = (std_name, 100.0)
                        logger.info(
                            f"Partial match found for '{column_name}' to '{std_name}' (variation: '{variation}')")

        if best_partial_match:
            return best_partial_match

        # Fuzzy matching (if no direct match)
        best_match = None
        best_score = 0.0

        for std_name, variations in self.standard_headers.items():
            for variation in variations:
                score = difflib.SequenceMatcher(None, normalized_column_name, variation.lower()).ratio() * 100
                if score > best_score:
                    best_score = score
                    best_match = std_name

        # Return match only if score is above threshold
        if best_score >= self.confidence_threshold:
            logger.info(f"Fuzzy match found for '{column_name}' to '{best_match}' with score: {best_score:.2f}")
            return best_match, best_score
        logger.warning(
            f"No match found for '{column_name}' with any standard header. Best fuzzy match score: {best_score:.2f}")
        return None, best_score



    def clean_cell_content(self, content: str) -> Tuple[str, int]:
        """
        Clean and standardize cell content
        Returns (cleaned_content, number_of_lines)
        """
        if not content:
            return "", 0

        # Remove extra whitespace and normalize spaces
        cleaned = re.sub(r'\s+', ' ', content.strip())

        # Count original number of lines
        num_lines = content.count('\n') + 1

        return cleaned, num_lines

    def extract_cell_text(self, cell_block: Dict, blocks: List[Dict]) -> str:
        """
        Extract text from cell relationships
        """
        text_parts = []

        if 'Relationships' not in cell_block:
            return ""

        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    for block in blocks:
                        if block['Id'] == child_id and block['BlockType'] == 'WORD':
                            text_parts.append(block['Text'])

        return ' '.join(text_parts)



    def process_table_test(self, blocks: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Textract blocks and extract table data.
        Handles cases where subsequent tables might not have a header row.
        Returns (processed_rows, unprocessed_rows)
        """
        tables = []
        current_table = None
        valid_columns = {}
        processed_rows = []
        unprocessed_rows = []
        table_count = 0

        for block in blocks:
            if block['BlockType'] == 'TABLE':
                table_count += 1
                if current_table:
                    tables.append(current_table)
                current_table = {
                    'table_number': table_count,
                    'cells': {},
                    'column_headers': {},
                    'num_rows': block.get('RowCount', 0),
                    'num_cols': block.get('ColumnCount', 0)
                }
                # Use existing headers if not first table
                if table_count > 1 and self.first_table_headers:
                    valid_columns = self.first_table_headers.copy()
                else:
                    valid_columns = {}

            elif block['BlockType'] == 'CELL' and current_table is not None:
                row_index = block.get('RowIndex', 0)
                col_index = block.get('ColumnIndex', 0)

                # Extract content and confidence
                raw_content = self.extract_cell_text(block, blocks)
                confidence = block.get('Confidence', 0.0)

                # Clean the content
                cleaned_content, num_lines = self.clean_cell_content(raw_content)

                # Process header row (row 1)
                if row_index == 1 and table_count <= 1:
                    std_name, match_score = self.standardize_column_name_test(cleaned_content)
                    if std_name:
                        valid_columns[col_index] = {
                            'standard_name': std_name,
                            'original_name': cleaned_content,
                            'confidence': confidence,
                            'match_score': match_score
                        }
                        # Store headers from first table
                        if table_count == 1:
                            self.first_table_headers = valid_columns.copy()
                else:
                    # Process data rows
                    if col_index in valid_columns:
                        cell_data = {
                            'content': cleaned_content,
                            'confidence': confidence,
                            'num_lines': num_lines
                        }

                        if current_table['cells'].get(row_index) is None:
                            current_table['cells'][row_index] = {}

                        current_table['cells'][row_index][valid_columns[col_index]['standard_name']] = cell_data

        # Process the last table
        if current_table and valid_columns:
            tables.append(current_table)

        # Convert table data to rows
        for table in tables:
            for row_idx, row_data in table['cells'].items():
                if  self._is_valid_row(row_data):
                    processed_row = {
                        'table_number': table['table_number'],
                        'row_number': row_idx,
                        'data': row_data
                    }
                    processed_rows.append(processed_row)
                else:
                    unprocessed_rows.append({
                        'table_number': table['table_number'],
                        'row_number': row_idx,
                        'data': row_data,
                        'reason': 'Missing mandatory fields or low confidence'
                    })

        return processed_rows, unprocessed_rows



    def _is_valid_row(self, row_data: Dict) -> bool:
        """
        Validate if row has mandatory fields and meets confidence threshold
        """
        # Check mandatory fields
        if not (row_data.get('jawatan') and row_data.get('nama')):
            return False

        # Check confidence levels
        for field_data in row_data.values():
            if field_data['confidence'] < self.confidence_threshold:
                return False

        return True


class PartyDirectoryProcessor:
    def __init__(self, bucket_name: str, root_directory: str, db_connection_string: str):
        self.navigator = S3DirectoryNavigator(bucket_name)
        self.root_directory = root_directory.rstrip('/')
        self.engine = create_engine(db_connection_string)
        Base.metadata.create_all(self.engine)
        #self.engine = self.create_tables(db_connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.textract_client = boto3.client('textract', region_name='ap-southeast-1')
        self.bucket_name = bucket_name
        self.table_processor = TextractTableProcessor()

        self.standard_columns = {
            "JAWATAN": ["jawatan", "position", "pangkat","JAWATAN DALAM PERTUBUHAN","JAWANTA","JAWAT"],
            "NAMA": ["nama", "name", "nama penuh"],
            "NO_KP": ["no. kad pengenalan", "no kp", "no. kp", "no. ic", "no ic","No K/P","Nombor Kad Pengenalan (Baru)", "NO. KAD PENGENALAN", "NO KP", "NO. KP", "NO IC",
                                      "NO. IC", "NOMBOR KAD PENGENALAN", "No K/P" , "NO K.P", "No K.P", "No.K.P.", "No.K.P"],
            "TELEFON": ["no telefon", "no. tel", "tel.", "telefon", "phone"],
            "ALAMAT": ["alamat", "address", "tempat tinggal"],
            "MAJIKAN": ["majikan",  "tempat kerja","perkerjaan, alamat majikan",],
            "TAHUN": ["tahun", "year", "tahun berkuatkuasa"],
            "JANTINA": ["jantina", "gender", "sex"]
        }

        # Configuration parameters
        self.min_match_score = 75
        self.confidence_threshold = 90.0
        self.max_line_length = 200

        # Processing state
        self.current_file_id = None
        self.current_page = None
        self.current_table = None

        error_handler = UnprocessedDataHandler(self.session)

    def create_tables(db_connection_string):
        """Creates the tables in the database."""
        try:
            engine = create_engine(db_connection_string)
            Base.metadata.create_all(engine)
            logger.info("Tables created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return None

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

    def process_sijil_specific_party(self, party_dir: str, party_info:PartyName):
        """Process Sijil directory for a party"""
        # base_path = f"{self.root_directory}/{party_dir}/Induk/SIJIL" #to use this
        base_path = f"{self.root_directory}/{party_dir}/Induk/Senarai AJK"
        result = {
            'directory_name': 'Sijil',
            'subdirectories': {}
        }

        self.process_years(base_path,party_dir,party_info)  # process year


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

    def process_parties(self, base_path: str):
        parties_directories = self.navigator.list_directories(f"{base_path}/")  # ['parties']

        # Process each directory
        for party in parties_directories:  # process every year
            print(f'party {party}')
            party_info = self.query_party_id(party)
            #self.process_sijil_specific_party(party,party_info)
            try:
                self.process_sijil_specific_party(party,party_info)
            except Exception as e:
                logger.error(f"Error in process_parties process: {str(e)}")
            #break

    def process_years(self, base_path: str, party_dir:str,party_info:PartyName):
        directories = self.navigator.list_directories(f"{base_path}/")  # ['years']

        # Process each directory
        for year in directories:  # process every year
            self.process_sijil_dir(base_path, year, party_dir,party_info)  # check for bendera directory

    def process_sijil_dir(self, base_path: str, year: str, party_dir:str,party_info:PartyName):
        #directories = self.navigator.list_directories(f"{base_path}/{year}/")  # ['years']

        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/")

        for pdf in pdfs:
            print(f"PDF: {pdf}")
            print(f"Path: {base_path}/{year}/{pdf}")
            self.process_ajk_pdf(f"{base_path}/{year}/{pdf}", base_path, year, pdf,party_dir,party_info)

    def process_ajk_dir(self, base_path: str, year: str, sijil_name: str):

        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/{sijil_name}/")

        for pdf in pdfs:
            try:
                print(f"PDF: {pdf}")
                print(f"Path: {base_path}/{year}/{sijil_name}/{pdf}")
                self.process_ajk_pdf(f"{base_path}/{year}/{sijil_name}/{pdf}", base_path, year, pdf)
            except Exception as e:
                logger.error(f"Error in process_ajk_dir process: {str(e)}")

    def extract_party_name(self, blocks):
        """
        Extract party name from the document using key-value pairs and line detection
        """
        party_name = None
        party_keywords = ["nama parti", "party name", "parti"]

        # Look through the blocks for key-value pairs
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text = block.get('Text', '').lower()

                # Check if line contains party keywords
                if any(keyword in text for keyword in party_keywords):
                    # Get the next line which should contain the party name

                    confidence = block.get('Confidence', 0)

                    # Only accept if confidence meets threshold
                    if confidence >= self.confidence_threshold:
                        party_name = text
                        return party_name
                    '''block_id = block.get('Id')
                    next_block = self.get_next_line_block(blocks, block_id)

                    if next_block:
                        party_name = next_block.get('Text', '').strip()
                        confidence = next_block.get('Confidence', 0)

                        # Only accept if confidence meets threshold
                        if confidence >= self.confidence_threshold:
                            return party_name

                    # Alternative: look for value in same line after colon
                    colon_split = text.split(':')
                    if len(colon_split) > 1:
                        party_name = colon_split[1].strip()
                        if party_name:
                            return party_name'''

        return party_name

    def get_next_line_block(self, blocks, current_block_id):
        """
        Helper method to get the next line block in the document
        """
        current_block_index = None

        # Find the current block index
        for i, block in enumerate(blocks):
            if block.get('Id') == current_block_id:
                current_block_index = i
                break

        if current_block_index is not None:
            # Look for the next LINE block
            for block in blocks[current_block_index + 1:]:
                if block['BlockType'] == 'LINE':
                    return block

        return None

    def _start_textract_job(self, bucket: str, document_key: str) -> Optional[str]:
        """
        Start an async Textract job for table detection
        """
        try:
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': document_key
                    }
                },
                FeatureTypes=['TABLES'],
                OutputConfig={
                    'S3Bucket': bucket,
                    'S3Prefix': 'textract-output'
                }
            )
            return response['JobId']
        except ClientError as e:
            logger.error(f"Error starting Textract job: {str(e)}")
            raise e

    def _wait_for_job_completion(self, job_id: str) -> str:
        """
        Wait for Textract job completion
        """
        max_attempts = 40  # 20 minutes maximum
        attempt = 0

        while attempt < max_attempts:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']

            if status in ['SUCCEEDED', 'FAILED']:
                return status

            attempt += 1
            time.sleep(30)  # Wait 30 seconds between checks

        return 'FAILED'

    def _get_job_results(self, job_id: str) -> List[Dict]:
        """
        Get results from completed Textract job
        """
        blocks = []
        pagination_token = None

        while True:
            if pagination_token:
                response = self.textract_client.get_document_analysis(
                    JobId=job_id,
                    NextToken=pagination_token
                )
            else:
                response = self.textract_client.get_document_analysis(JobId=job_id)

            blocks.extend(response['Blocks'])

            if 'NextToken' in response:
                pagination_token = response['NextToken']
            else:
                break

        return blocks

    def process_ajk_pdf(self, path: str, base_path: str, year: str, pdf: str, party_dir: str,party_info:PartyName):
        try:

            existing_file = self.session.query(ProcessedFile).filter_by(
                s3_path=path
            ).first()

            if existing_file and existing_file.is_processed:
                logger.info(f"Skipping already processed file: {path}")
                return False

            job_id = self._start_textract_job(self.bucket_name, path)
            if not job_id:
                raise Exception("Failed to start Textract job")

            # Wait for job completion
            job_status = self._wait_for_job_completion(job_id)
            if job_status != 'SUCCEEDED':
                raise Exception(f"Textract job failed with status: {job_status}")

            # Get results
            blocks = self._get_job_results(job_id)

            # Process tables

            processed_rows, unprocessed_rows = self.table_processor.process_table_test(blocks)

            # Save to database
            self._save_results(processed_rows, unprocessed_rows, self.bucket_name, path, party_dir,year,path,party_info)

            if len(unprocessed_rows) <= 0 and len(processed_rows) <= 0:
                value_error = Exception(f"Error processing document. Possibly not properly scan ")
                logger.error(f"Error processing document {path}: {str(value_error)}")
                self.error_handler.handle_extraction_error(self.bucket_name, path, value_error)

            processed_file = existing_file or ProcessedFile(
                s3_path=path,
                filename=pdf,
                year_directory=year,
                sijil_type=SijilType.AJK.value
            )

            processed_file.is_processed = True
            self.session.add(processed_file)
            logger.info(f"Completed processing document: s3://{self.bucket_name}/{path}")
            self.session.commit()




        except Exception as e:
            self.session.rollback()
            unprocessed = UnprocessedData(
                s3_bucket=self.bucket_name,
                s3_key=path,
                page_number=1,
                error_message=ProcessingErrorType.EXTRACTION_FAILED.value,
                sijil_type=SijilType.AJK.value
            )
            error_context = {
                "error_message": str(e),
            }
            self.saved_to_unprocess_data(unprocessed, error_context)
            logger.error(f"Error in main process: {str(e)}")
            raise


    def saved_to_unprocess_data(self, unprocessed: UnprocessedData, error_context: dict):
        unprocessed.log_error(self.session, ProcessingErrorType.LOW_CONFIDENCE, error_context)

    def _save_results(self, processed_rows: List[Dict], unprocessed_rows: List[Dict],
                      bucket: str, document_key: str, party_dir:str, year:str, path:str, party_info:PartyName) -> None:
        """
        Save processed and unprocessed results to database
        """
        try:
            # Save processed rows
            for row in processed_rows:
                data = row['data']
                if 'jawatan' in data['jawatan']['content'] .lower()  and 'nama' in data['nama']['content'].lower():
                    continue
                ajk = AJK(
                    jawatan=data['jawatan']['content'],
                    nama=data['nama']['content'],
                    no_kp=data.get('no_kp', {}).get('content'),
                    no_telefon=data.get('no_telefon', {}).get('content'),
                    alamat=data.get('alamat', {}).get('content'),
                    majikan=data.get('majikan', {}).get('content'),
                    tahun=year,
                    jantina=data.get('jantina', {}).get('content'),
                    pekerjaan=data.get('pekerjaan', {}).get('content'),
                    party_name=party_dir,
                    path = path,
                    confidence_score=min(
                        float(data[field]['confidence'])
                        for field in data
                        if 'confidence' in data[field]
                    )
                )
                if party_info:
                    ajk.party_id = party_info.id
                self.session.add(ajk)

            # Save unprocessed rows
            for row in unprocessed_rows:
                unprocessed = UnprocessedData(
                    s3_bucket=bucket,
                    s3_key=document_key,
                    page_number=1,  # Update if handling multiple pages
                    row_number=row['row_number'],
                    error_type=ProcessingErrorType.LOW_CONFIDENCE.value,
                    error_message=row['reason'],
                    error_details=json.dumps(row['data']),
                    confidence_score=min(
                        float(data['confidence'])
                        for data in row['data'].values()
                        if 'confidence' in data
                    )
                )
                self.session.add(unprocessed)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving results to database: {str(e)}")
            raise

    def process_ajk_pdf_ori(self, path: str, base_path: str, year: str, pdf: str, party_dir:str):
        """Process a single PDF file from S3 with enhanced error handling"""
        s3_path = f'{base_path}/{year}/{pdf}'
        party_name = None
        processed_file = None
        try:
            # Check if file was already processed
            '''existing_file = self.session.query(ProcessedFile).filter_by(
                s3_path=s3_path
            ).first()

            if existing_file and existing_file.is_processed:
                logger.info(f"Skipping already processed file: {s3_path}")
                return'''
            existing_file = s3_path
            # Start Textract job
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': s3_path
                    }
                },
                #FeatureTypes=['TABLES', 'LAYOUT']
                FeatureTypes=['TABLES', 'LAYOUT']
            )

            job_id = response['JobId']
            logger.info(f"Started Textract job {job_id} for {s3_path}")

            # Create or update processed file record
            '''processed_file = existing_file or ProcessedFile(
                s3_path=s3_path,
                filename=os.path.basename(s3_path),
                year_directory=year_dir
            )

            if not existing_file:
                self.session.add(processed_file)
                self.session.flush()'''

            processed_file = ProcessedFile(
                s3_path=s3_path,
                filename=os.path.basename(s3_path),
                year_directory=year
            )
            if not existing_file:
                self.session.add(processed_file)
                self.session.flush()

            self.session.add(processed_file)
            self.session.flush()
            #processed_file.id = 0

            #self.current_file_id = processed_file.id

            # Get all pages data
            pages_data = self.wait_and_get_results(job_id)
            processed_file.total_pages = len(pages_data)
            '''processed_file = {
                'last_processed_page': 0,
                'processed_pages': 0,
                'is_processed': False,
                'error_message': None
            }'''
            #processed_file.last_processed_page= 0
            # Process each page
            for page_num, page_data in enumerate(pages_data, 1):
                if page_num <= processed_file.last_processed_page:
                    logger.info(f"Skipping already processed page {page_num}")
                    continue

                try:
                    self.current_page = page_num
                    if  party_name is None:
                        party_name = self.extract_party_name(page_data['Blocks'])
                        logger.info(f"Extracted party name: {party_name}")
                    tables = self.extract_table_data(page_data['Blocks'], page_num, s3_path)
                    logger.info('test')

                    for table_num, table in enumerate(tables, 1):
                        self.current_table = table_num
                        self.store_extracted_data(page_num, table, party_name , year,f'{base_path}/{year}/{pdf}')

                    processed_file.last_processed_page = page_num
                    processed_file.processed_pages += 1
                    self.session.commit()
                    logger.info(f"Processed page {page_num} of {len(pages_data)}")

                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {str(e)}")
                    continue

            processed_file.is_processed = True
            self.session.commit()
            logger.info(f"Successfully processed {s3_path}")

        except Exception as e:
            logger.error(f"Error processing {s3_path}: {str(e)}")
            '''if processed_file:
                processed_file.error_message = str(e)
                self.session.commit()
            self.session.rollback()'''
            raise

    def extract_table_data(self, blocks: List[Dict], page_num: int, s3_path:str) -> List[Dict]:
        """Extract and process table data from Textract blocks"""
        tables = []
        current_table = None
        valid_columns = {}
        table_count = 0
        unprocessed_batch = []

        for block in blocks:
            if block['BlockType'] == 'TABLE':
                table_count += 1
                if current_table and valid_columns:
                    tables.append(current_table)
                current_table = {
                    'table_number': table_count,
                    'cells': {},
                    'column_headers': {},
                    'num_rows': block.get('RowCount', 0),
                    'num_cols': block.get('ColumnCount', 0)
                }
                valid_columns = {}

            elif block['BlockType'] == 'CELL' and current_table is not None:
                row_index = block.get('RowIndex', 0)
                col_index = block.get('ColumnIndex', 0)

                # Extract raw content and confidence
                raw_content = self.extract_cell_text(block, blocks)
                confidence = block.get('Confidence', 0.0)

                # Clean the content
                cleaned_content, num_lines = self.clean_cell_content(raw_content)

                # Process header row (row 1) - Don't store headers in UnprocessedCells
                if row_index == 1:
                    std_name, match_score = self.standardize_column_name(cleaned_content)
                    if std_name:
                        valid_columns[col_index] = {
                            'name': std_name,
                            'match_score': match_score,
                            'original': cleaned_content
                        }
                        current_table['column_headers'][col_index] = valid_columns[col_index]
                    continue  # Skip storing headers in unprocessed cells

                # Process data cells (rows 2 and beyond)
                if row_index > 1:
                    # For cells in columns that don't match standard headers
                    if col_index not in valid_columns:
                        unprocessed_batch.append({
                            'page_num': page_num,
                            'table_num': table_count,
                            'row_num': row_index,
                            'column_name': f"Column_{col_index}",
                            'content': raw_content,  # Store the cell value
                            'confidence': confidence,
                            'reason': "Column not in standard columns"
                        })
                        continue

                    # For cells with low confidence scores
                    if confidence < self.confidence_threshold:
                        unprocessed_batch.append({
                            'page_num': page_num,
                            'table_num': table_count,
                            'row_num': row_index,
                            'column_name': valid_columns[col_index]['name'],
                            'content': raw_content,  # Store the cell value
                            'confidence': confidence,
                            'reason': "Low confidence score"
                        })
                        continue

                    # For empty or invalid content
                    if not cleaned_content:
                        unprocessed_batch.append({
                            'page_num': page_num,
                            'table_num': table_count,
                            'row_num': row_index,
                            'column_name': valid_columns[col_index]['name'],
                            'content': raw_content,  # Store the cell value
                            'confidence': confidence,
                            'reason': "Empty or invalid content"
                        })
                        continue

                    # Store valid cell data
                    current_table['cells'][(row_index, col_index)] = {
                        'content': cleaned_content,
                        'confidence': confidence,
                        'num_lines': num_lines,
                        'column_name': valid_columns[col_index]['name']
                    }

                # Process unprocessed cells in batches
                if len(unprocessed_batch) >= 100:
                    self.store_unprocessed_cells_batch(unprocessed_batch, s3_path)
                    unprocessed_batch = []

        # Process any remaining unprocessed cells
        if unprocessed_batch:
            self.store_unprocessed_cells_batch(unprocessed_batch,s3_path)

        if current_table and valid_columns:
            tables.append(current_table)

        return tables

    def standardize_column_name(self, original_name: str) -> Tuple[str, float]:
        """Find the closest matching standard column name with enhanced matching"""



        if not original_name:
            return "", 0.0

        cleaned_name = ' '.join(original_name.strip().split()).lower()
        best_match = ("", 0.0)

        if "majikan" in cleaned_name or "employer" in cleaned_name:
            return "MAJIKAN", 100.0


        # Try exact match first
        for standard_name, variations in self.standard_columns.items():
            if cleaned_name in variations or standard_name in cleaned_name  or cleaned_name == standard_name.lower():
                return standard_name, 100.0

        # If no exact match, try fuzzy matching
        for standard_name, variations in self.standard_columns.items():
            # Check against standard name
            score = fuzz.token_sort_ratio(cleaned_name, standard_name.lower())
            if score > best_match[1] and score >= self.min_match_score:
                best_match = (standard_name, score)

            # Check against variations
            for variation in variations:
                score = fuzz.token_sort_ratio(cleaned_name, variation)
                if score > best_match[1] and score >= self.min_match_score:
                    best_match = (standard_name, score)

        for standard_name, variations in self.standard_columns.items():
            for variation in variations:
                if variation.lower() in cleaned_name:
                    return standard_name, 95.0  # return 95 if contain

        return best_match

    def is_signature_line(self, text: str) -> bool:
        """Check if text is part of signature section"""
        signature_indicators = [
            'tandatangan', 'signature', 't/tangan', 't.t', 't/t',
            'cop', 'stamp', 'seal', 'meterai', 'disahkan',
            'pengesahan', 'certified', 'verified', 'endorsed'
        ]
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in signature_indicators)

    def clean_cell_content(self, content: str) -> Tuple[str, int]:
        """Clean cell content and handle newlines"""
        if not content:
            return "", 0

        lines = [line.strip() for line in content.split('\n')]
        cleaned_lines = []

        for line in lines:
            if line and not self.is_signature_line(line):
                # Remove multiple spaces
                line = ' '.join(line.split())
                if len(line) <= self.max_line_length:
                    cleaned_lines.append(line)

        if not cleaned_lines:
            return "", 0

        return '\n'.join(cleaned_lines), len(cleaned_lines)
    def extract_cell_text(self, cell_block: Dict, blocks: List[Dict]) -> str:
        """Extract text from cell block"""
        text_parts = []

        if 'Relationships' in cell_block:
            for rel in cell_block['Relationships']:
                if rel['Type'] == 'CHILD':
                    for word_id in rel['Ids']:
                        word_block = next((b for b in blocks if b['Id'] == word_id), None)
                        if word_block and word_block['BlockType'] == 'WORD':
                            text_parts.append(word_block['Text'])

        return ' '.join(text_parts)

    def store_extracted_data(self, page_num: int, table_data: Dict,party_name:str , year: str, path:str):
        """Store extracted table data in the database"""
        try:
            #key_value = {}
            if not table_data.get('cells'):
                logger.warning(f"No cells found in table on page {page_num}")
                return

            stored_count = 0
            for (row_idx, col_idx), cell_data in table_data['cells'].items():
                if col_idx not in table_data['column_headers']:
                    continue

                column_info = table_data['column_headers'][col_idx]

                # Skip if content is empty
                if not cell_data.get('content'):
                    continue

                #key_value[column_info['name']] = cell_data['content']

                extracted_data = ExtractedTableData(
                    #file_id=self.current_file_id,
                    page_number=page_num,
                    table_number=table_data['table_number'],
                    row_number=row_idx,
                    column_name=column_info['name'],  # Changed from 'standardized'
                    content=cell_data['content'],
                    confidence_score=cell_data['confidence'],
                    path=path
                )

                self.session.add(extracted_data)
                stored_count += 1

                # Commit in batches to improve performance
                if stored_count % 100 == 0:
                    self.session.flush()



            '''logger.info('hey')
            logger.info(json.dumps(key_value, indent=4))
            ajkData = AJKExtractedData()
            ajkData.parti = party_name
            ajkData.tahun = year
            ajkData.pdf_location = path
            ajkData.jawatan = key_value['JAWATAN']
            ajkData.nama = key_value['NAMA']
            ajkData.no_kp = key_value['NO_KP']
            ajkData.alamat = key_value['ALAMAT']
            ajkData.majikan = key_value['MAJIKAN']'''

            #ajkData.add(key_value)
            #self.session.add(ajkData)
            #self.session.commit()

            if stored_count > 0:
                self.store_ajk_data(table_data, party_name, year, path)
                self.session.commit()
                logger.info(f"Stored {stored_count} rows for page {page_num}, table {table_data['table_number']}")
            else:
                logger.warning(f"No valid data to store for page {page_num}, table {table_data['table_number']}")

        except Exception as e:
            logger.error(f"Error storing extracted data: {str(e)}")
            self.session.rollback()
            raise

    def store_ajk_data(self, table_data: Dict, party_name: str, year: str, path: str):
        """Store AJK data from table rows"""
        try:
            # Dictionary to store row data
            row_data = {}

            # Collect data by rows
            for (row_idx, col_idx), cell_data in table_data['cells'].items():
                if col_idx not in table_data['column_headers']:
                    continue

                if not cell_data.get('content'):
                    continue

                column_name = table_data['column_headers'][col_idx]['name']
                content = cell_data['content']

                if row_idx not in row_data:
                    row_data[row_idx] = {}
                row_data[row_idx][column_name] = content

            # Column mapping dictionary
            column_mapping = {
                'JAWATAN': 'jawatan',
                'NAMA': 'nama',
                'NO_KP': 'no_kp',
                'NO TELEFON': 'telepon',
                'ALAMAT': 'alamat',
                'MAJIKAN': 'majikan',
                'JANTINA': 'jantina'
            }

            stored_count = 0
            # Process each row
            for row_idx, row in row_data.items():
                ajk_data = {}

                # Map the columns
                for source_col, target_col in column_mapping.items():
                    if source_col in row:
                        ajk_data[target_col] = row[source_col]

                # Add additional fields
                #ajk_data['tahun'] = year
                ajk_data['parti'] = party_name
                ajk_data['pdf_location'] = path

                # Check for required fields
                if 'nama' in ajk_data and 'no_kp' in ajk_data:
                    # Check for duplicates
                    existing_record = self.session.query(AJKExtractedData).filter(
                        AJKExtractedData.nama == ajk_data['nama'],
                        AJKExtractedData.no_kp == ajk_data['no_kp'],
                        AJKExtractedData.tahun == year,
                        AJKExtractedData.parti == party_name
                    ).first()

                    if existing_record:
                        # Update existing record
                        for key, value in ajk_data.items():
                            setattr(existing_record, key, value)
                        logger.info(f"Updated existing AJK record for {ajk_data['nama']}")
                    else:
                        # Create new record
                        ajk_record = AJKExtractedData(**ajk_data)
                        self.session.add(ajk_record)
                        stored_count += 1
                else:
                    logger.warning(f"Skipping incomplete AJK record on row {row_idx}: missing nama or no_kp")

            if stored_count > 0:
                logger.info(f"Added {stored_count} new AJK records")

        except Exception as e:
            logger.error(f"Error storing AJK data: {str(e)}")
            raise

    def store_unprocessed_cells_batch(self, unprocessed_batch: List[Dict],s3_path:str):
        """Store a batch of unprocessed cells with their values"""
        try:
            unprocessed_records = []
            for cell in unprocessed_batch:
                unprocessed = UnprocessedCells(
                    #ckf file_id=self.current_file_id,
                    page_number=cell['page_num'],
                    table_number=cell['table_num'],
                    row_number=cell['row_num'],
                    column_name=cell['column_name'],
                    content=cell['content'],  # Store the actual cell value
                    confidence_score=cell['confidence'],
                    reason=cell['reason'],
                    path=s3_path
                )
                unprocessed_records.append(unprocessed)

            # Bulk insert the records
            try:
                self.session.bulk_save_objects(unprocessed_records)
                self.session.flush()
                logger.debug(f"Stored {len(unprocessed_records)} unprocessed cells in batch")
            except Exception as flush_error:
                logger.error(f"Error flushing unprocessed cells batch: {str(flush_error)}")
                self.session.rollback()
                raise

        except Exception as e:
            logger.error(f"Error storing unprocessed cells batch: {str(e)}")
            self.session.rollback()
            raise

    def wait_and_get_results(self, job_id: str) -> List[Dict]:
        """Wait for job completion and get all results"""
        # Wait for job completion
        while True:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']

            if status == 'SUCCEEDED':
                break
            elif status == 'FAILED':
                raise Exception(f"Textract job failed: {response.get('StatusMessage')}")
            elif status in ['IN_PROGRESS', 'SUBMITTED']:
                time.sleep(5)
                continue
            else:
                raise Exception(f"Unknown job status: {status}")

        # Get all results
        pages_data = []
        next_token = None

        while True:
            try:
                if next_token:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token,
                        MaxResults=1000
                    )
                else:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        MaxResults=1000
                    )

                if 'Blocks' in response:
                    pages_data.append(response)

                next_token = response.get('NextToken')
                if not next_token:
                    break

                time.sleep(0.25)  # Avoid throttling

            except Exception as e:
                logger.error(f"Error getting results for job {job_id}: {str(e)}")
                raise

        return pages_data


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
    #db_connection = "mysql+mysqlconnector://dbeaver:dbeaver@127.0.0.1:3306/eroses_dev"
    #db_connection = "mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev"
    db_connection = "mysql+mysqlconnector://admin:Eroses123@rds-erosesrdsinstance-bl01iw3u4yka.c1q0w0yu2dv3.ap-southeast-5.rds.amazonaws.com:3306/eroses_migration"
    '''bucket_name = 'digitization-migration'
    root_directory = 'Parti_Politik-Induk'''
    bucket_name = 'induk-account-training'
    #root_directory = 'Parti_Politik-Induk'
    root_directory = 'Parti_Politik-Induk_Modified'

    processor = PartyDirectoryProcessor(bucket_name, root_directory, db_connection)
    processor.process_parties(root_directory)
    # result = processor.process_all_parties()
    # print(result)
    #processor.process_sijil_specific_party("BARISAN KEMAJUAN INDIA SE-MALAYSIA")
    #ori processor.process_sijil_specific_party("AMANAH")

    # Save result to JSON file
    import json


if __name__ == "__main__":
    main()
