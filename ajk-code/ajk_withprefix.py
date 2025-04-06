import boto3
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean,Boolean, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import logging
import re
from typing import Optional
from botocore.config import Config

from helper.aws_textparser import TextractorHelper
#from repository import AJK

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import urllib.parse
import time

from typing import List, Dict
from botocore.exceptions import ClientError
import json
# SQLAlchemy setup
#Base = declarative_base()





# SQLAlchemy setup
Base = declarative_base()


class AJK(Base):
    __tablename__ = "ajk"
    id    = Column(Integer, primary_key=True)
    society_name = Column(String(255),nullable=True)
    name  = Column(String(250),nullable=True)
    address = Column(String(250),nullable=True)
    position = Column(String(100),nullable=True)
    identification_number = Column(String(50),nullable=True)
    phone_number = Column(String(250),nullable=True)
    dob = Column(String(50),nullable=True)
    gender = Column(String(10),nullable=True)
    path = Column(String(500), nullable=False)
    employer =  Column(String(500), nullable=True)
    create_at = Column(DateTime, default=datetime.now),
    update_at = Column(DateTime, default=datetime.now),
    created_by = Column(String(250), nullable=True)
    updated_by = Column(String(250), nullable=True)

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
    error_message = Column(Text)


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


class AJKExtractedData(Base):
    __tablename__ = 'ajk_extracted_data'

    id = Column(Integer, primary_key=True)
    jawatan = Column(String(255), nullable=True)
    nama = Column(String(255), nullable=True)
    no_kp = Column(String(255), nullable=True)
    telepon = Column(String(255), nullable=True)
    alamat = Column(String(255), nullable=True)
    majikan = Column(String(255), nullable=True)
    tahun = Column(String(255), nullable=True)
    parti =  Column(String(255), nullable=True)
    jantina = Column(String(255), nullable=True)
    pdf_location = Column(String(1000), nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)




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
        #self.engine = self.create_tables(db_connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.textract_client = boto3.client('textract', region_name='ap-southeast-1')
        self.bucket_name = bucket_name

        self.standard_columns = {
            "JAWATAN": ["jawatan", "position", "pangkat"],
            "NAMA": ["nama", "name", "nama penuh"],
            "NO_KP": ["no. kad pengenalan", "no kp", "no. kp", "no. ic", "no ic","No K/P"],
            "TELEFON": ["no telefon", "no. tel", "tel.", "telefon", "phone"],
            "ALAMAT": ["alamat", "address", "tempat tinggal"],
            "MAJIKAN": ["majikan", "tempat kerja","perkerjaan dan alamat majikan", "alamat majikan"],
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

    def process_sijil_specific_party(self, party_dir: str):
        """Process Sijil directory for a party"""
        # base_path = f"{self.root_directory}/{party_dir}/Induk/SIJIL" #to use this
        base_path = f"{self.root_directory}/{party_dir}/Induk/Senarai AJK"
        result = {
            'directory_name': 'Sijil',
            'subdirectories': {}
        }

        self.process_years(base_path, party_dir)  # process year

    def process_years(self, base_path: str, party_dir:str):
        directories = self.navigator.list_directories(f"{base_path}/")  # ['years']

        # Process each directory
        for year in directories:  # process every year
            self.process_sijil_dir(base_path, year, party_dir)  # check for bendera directory

    def process_sijil_dir(self, base_path: str, year: str, party_dir:str):
        #directories = self.navigator.list_directories(f"{base_path}/{year}/")  # ['years']

        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/")

        for pdf in pdfs:
            print(f"PDF: {pdf}")
            print(f"Path: {base_path}/{year}/{pdf}")
            self.process_ajk_pdf_2(f"{base_path}/{year}/{pdf}", base_path, year, pdf, party_dir)



    '''def extract_society_name(self, input_string:str):
        
        society_name = ''
        if match := re.search(
            r"PARTY\s*:\s*(.*?)\s*(\n|$)", input_string, re.IGNORECASE
        ):
            return match[1].strip()'''

    def extract_society_name_after_keyword(self, input_string:str):
        """
        Extracts the society name from the input string after the first occurrence of "NAMA PERTUBUHAN".
        """
        match = re.search(r"NAMA PERTUBUHAN\s*:\s*(.*?)\s*(\n|$)", input_string, re.IGNORECASE)  # Corrected regex
        if match:
            society_name = match.group(1).strip()
            return society_name
        return None  # Or raise an exception if the name is required

    def extract_society_name(self, input_string: str):
        """
        Extracts the party name including the word "Party" or "Parti" (case-insensitive)
        from the input string until the end of the line.

        Args:
            input_string: The input string to search within.

        Returns:
            The extracted party name (including "Party" or "Parti") or None if not found.
        """
        # Regular expression to find "Party" or "Parti" followed by any characters until the end of the line.
        # \b ensures that we match whole words (e.g., "Party" but not "Multiparty").
        # (.*) captures the party name after "Party" or "Parti".
        # $ matches the end of the line.
        # re.IGNORECASE makes the search case-insensitive.
        pattern = r"\b(Party|Parti)\b\s*(.*?)$"

        # Search for the pattern in the input string.
        for line in input_string.splitlines():
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                # If a match is found, return the entire matched string (group 0).
                return match.group(0).strip()

        # If no match is found, return None.
        return None

    def process_ajk_pdf_2(self, pdf_path: str, base_path: str, year: str, pdf_name: str,societyname=None):
        try:
            parser = TextractorHelper()
            bucket_name = f"s3://{self.bucket_name}"
            s3_path = f'{bucket_name}/{base_path}/{year}/{pdf_name}'
            #text = parser.start_ajk_analysis('s3://induk-account-training/ajk/68C2019-1.pdf')
            text = parser.start_ajk_analysis(s3_path)
            parsed_text = parser.get_text(text, 'table')
            #society_name = self.extract_society_name(parsed_text)
            society_name = societyname
            #str = self.extract_society_name_after_keyword(parsed_text)
            if (temp_name := self.extract_society_name_after_keyword(parsed_text)) is not None and temp_name != '':
                society_name = temp_name
            elif (temp_name := self.extract_society_name(parsed_text)) is not None and temp_name != '':
                society_name = temp_name
            self.extract_and_save_to_db_1(parsed_text, society_name , s3_path)#, pdf_name)society_name)  # Extract a
        except Exception as e:
            logger.error(e)

    '''def extract_and_save_to_db_1(self,input_string, society_name="Unknown Society"):
        table_prefix = "_starttblprefix"
        table_suffix = "_tblsuffix"
        row_prefix = "_tr"
        row_suffix = "_tm"
        cell_prefix = "_td"
        cell_suffix = "_tx"
        column_mapping = {  # Updated and corrected mapping
            "JAWATAN DALAM PERTUBUHAN": "position",
            "NAMA PENUH (Nama dan alamat huruf Cina bagi nama orang Cina)": "name",
            "ALAMAT PENUH TEMPAT TINGGAL": "address",
            "L/P": "gender",
            "Nombor Kad Pengenalan (Baru)": "identification_number",
            "NO. TELEFON": "phone_number",
            "TARIKH LAHIR Dan TEMPAT LAHIR": "dob",
        }

        table_strings = re.findall(rf"{table_prefix}(.*?){table_suffix}", input_string, re.DOTALL)
        engine = create_engine("mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev")
        session = sessionmaker(bind=engine)
        session = session()  # Create a session

        for table_string in table_strings:
            rows_data = table_string.strip().split(row_prefix)[1:]

            header_row = None  # Initialize header row to None
            for row_data in rows_data:
                cells_data = row_data.split(row_suffix)[0].split(cell_prefix)
                row = [cell.split(cell_suffix)[0].strip() for cell in cells_data[1:]]

                if header_row is None:  # Check if the header row has been processed
                    header_row = row
                    continue  # Skip processing this row as data

                ajk_data = {"society_name": society_name}
                for i, cell_value in enumerate(row):
                    header_name = header_row[i]  # Now you have header name from header_row
                    if header_name in column_mapping:
                        ajk_data[column_mapping[header_name]] = cell_value

                ajk_entry = AJK(**ajk_data)
                session.add(ajk_entry)

        session.commit()
        session.close()'''

    def extract_and_save_to_db_1(self, input_string: str, society_name: str = "Unknown Society", s3_path: str = None) -> None:
        """
        Extracts AJK data from tables in the input string and saves it to the database.

        Args:
            input_string: The string containing the table data.
            society_name: The name of the society.
        """
        table_prefix = "_starttblprefix"
        table_suffix = "_tblsuffix"
        row_prefix = "_tr"
        row_suffix = "_tm"
        cell_prefix = "_td"
        cell_suffix = "_tx"

        # Enhanced column mapping with multiple keys per column
        column_mapping: Dict[str, List[str]] = {
            "position": ["JAWATAN DALAM PERTUBUHAN", "JAWATAN", "POSITION", "PANGKAT","JAWANTA","JAWAT"],
            "name": ["NAMA PENUH (Nama dan alamat huruf Cina bagi nama orang Cina)", "NAMA PENUH", "NAMA", "NAME"],
            "address": ["ALAMAT PENUH TEMPAT TINGGAL", "ALAMAT", "ADDRESS", "TEMPAT TINGGAL"],
            "gender": ["L/P", "GENDER", "JANTINA"],
            "identification_number": ["Nombor Kad Pengenalan (Baru)", "NO. KAD PENGENALAN", "NO KP", "NO. KP", "NO IC",
                                      "NO. IC", "NOMBOR KAD PENGENALAN", "No K/P" , "NO K.P", "No K.P", "No.K.P.", "No.K.P"],
            "phone_number": ["NO. TELEFON", "TELEFON", "NO TELEFON", "PHONE NUMBER", "PHONE", "NO. TEL", "TEL.", "NO TELEFON"],
            "dob": ["TARIKH LAHIR Dan TEMPAT LAHIR", "TARIKH LAHIR", "DATE OF BIRTH", "DOB", "TEMPAT LAHIR"],
        }

        table_strings = re.findall(rf"{table_prefix}(.*?){table_suffix}", input_string, re.DOTALL)
        #engine = create_engine("mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev")
        #Session = sessionmaker(bind=engine)
        #session = Session()

        try:
            for table_string in table_strings:
                rows_data = table_string.strip().split(row_prefix)[1:]

                header_row: Optional[List[str]] = None
                for row_data in rows_data:
                    cells_data = row_data.split(row_suffix)[0].split(cell_prefix)
                    row = [cell.split(cell_suffix)[0].strip() for cell in cells_data[1:]]

                    if header_row is None:
                        header_row = row
                        continue

                    ajk_data: Dict[str, str] = {"society_name": society_name}
                    for i, cell_value in enumerate(row):
                        if i < len(header_row):
                            header_name = header_row[i]
                            # Find the corresponding key in column_mapping
                            mapped_key = self.get_mapped_key(header_name, column_mapping)
                            if mapped_key == 'name':
                                logger.info('name')
                            if mapped_key == 'name' and (cell_value is None  or cell_value =='' or len(cell_value) < 2):
                                continue
                            if cell_value == 'Jawatan :':
                                logger.info('Jawatan :')
                            if mapped_key:
                                ajk_data[mapped_key] = cell_value

                    if 'name' in ajk_data and 'position' in  ajk_data:
                        ajk_data['path'] = s3_path
                        ajk_entry = AJK(**ajk_data)
                        self.session.add(ajk_entry)

            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        #finally:
        #    self.session.close()

    '''def get_mapped_key(self, header_name: str, column_mapping: Dict[str, List[str]]) -> Optional[str]:
        """
        Finds the mapped key in column_mapping based on the header_name.

        Args:
            header_name: The header name from the table.
            column_mapping: The mapping dictionary.

        Returns:
            The mapped key (e.g., "position") or None if not found.
        """
        header_name_upper = header_name.upper()
        for mapped_key, possible_names in column_mapping.items():
            for possible_name in possible_names:
                if header_name_upper == possible_name.upper():
                    return mapped_key
        return None'''

    def get_mapped_key(self, header_name: str, column_mapping: Dict[str, List[str]]) -> Optional[str]:
        """
        Finds the mapped key in column_mapping based on the header_name.
        Now checks if the header_name *contains* any of the possible names.

        Args:
            header_name: The header name from the table.
            column_mapping: The mapping dictionary.

        Returns:
            The mapped key (e.g., "position") or None if not found.
        """
        header_name_upper = header_name.upper()
        for mapped_key, possible_names in column_mapping.items():
            for possible_name in possible_names:
                # Check for exact match (for efficiency)

                if ("MAJIKAN" in header_name_upper.upper()) or ("EMPLOYER" in header_name_upper.upper()) :
                    return "employer"

                if header_name_upper == possible_name.upper():
                    return mapped_key
                # Check if header_name contains possible_name (case-insensitive)
                if possible_name.upper() in header_name_upper:
                    return mapped_key
                # Check if possible_name contains header_name (case-insensitive)
                '''if header_name_upper in possible_name.upper():
                    return mapped_key'''



        return None


    def process_ajk_dir(self, base_path: str, year: str, sijil_name: str):

        pdfs = self.navigator.list_pdfs(f"{base_path}/{year}/{sijil_name}/")

        for pdf in pdfs:
            print(f"PDF: {pdf}")
            print(f"Path: {base_path}/{year}/{sijil_name}/{pdf}")
            self.process_ajk_pdf(f"{base_path}/{year}/{sijil_name}/{pdf}", base_path, year, pdf)

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

    def process_ajk_pdf(self, path: str, base_path: str, year: str, pdf: str):
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
                        party_name = self.extract_party_name(page_data['Blocks']) #ckf process party name
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

        # Try exact match first
        for standard_name, variations in self.standard_columns.items():
            if cleaned_name in variations or cleaned_name == standard_name.lower():
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
                ajk_data['tahun'] = year
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
    #windows db_connection = "mysql+mysqlconnector://dbeaver:dbeaver@127.0.0.1:3306/eroses_dev"
    db_connection = "mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev"
    '''bucket_name = 'digitization-migration'
    root_directory = 'Parti_Politik-Induk'''


    '''ori bucket_name = 'induk-account-training'
    root_directory = 'Parti_Politik-Induk'
    processor = PartyDirectoryProcessor(bucket_name, root_directory, db_connection)'''

    bucket_name = 'induk-account-training'
    root_directory = 'Parti_Politik-Induk_Modified'
    processor = PartyDirectoryProcessor(bucket_name, root_directory, db_connection)

    # result = processor.process_all_parties()
    # print(result)
    processor.process_sijil_specific_party("AMANAH")
    #processor.process_sijil_specific_party("BARISAN KEMAJUAN INDIA SE-MALAYSIA")

    # Save result to JSON file
    import json


if __name__ == "__main__":
    main()
