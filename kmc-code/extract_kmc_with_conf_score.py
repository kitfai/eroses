import boto3
import time
import json
import re
from sqlalchemy import create_engine, Column, Integer, String, Text, Numeric, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from colorama import Fore, Style, init

# Initialize Colorama for cross-platform colored output
init(autoreset=True)

Base = declarative_base()

# Table model for unprocessed documents
class UnprocessedDoc(Base):
    __tablename__ = 'documents_for_manual_reprocessing'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(255), nullable=False, unique=True)
    reason = Column(String(255))
    extraction_confidence = Column(Numeric(precision=5, scale=2))
    processed_at = Column(DateTime, default=func.now())


# Table model for sub-processed documents (maybe few rows failed)
class PartialBranchInfoReview(Base):
    __tablename__ = 'branch_info_for_manual_review'

    id = Column(Integer, primary_key=True)
    s3_path = Column(String(255), nullable=False)
    page_number = Column(Integer)
    row_index = Column(Integer)
    raw_column_1_text = Column(Text)
    bil = Column(String(10))
    raw_column_2_text = Column(Text)
    parsed_rujukan_ppm = Column(String(255))
    raw_column_3_text = Column(Text)
    parsed_nama_cawangan = Column(String(255))
    raw_column_4_text = Column(Text)
    parsed_alamat_cawangan = Column(Text)
    state = Column(String(50))
    reason = Column(String(255))
    processed_at = Column(DateTime, default=func.now())


# Table model for branch info
class BranchInfo(Base):
    __tablename__ = 'branch_info'
    id = Column(Integer, primary_key=True)
    s3_path = Column(String(255), nullable=False)
    bil = Column(String(10))
    rujukan_ppm = Column(String(255))
    nama_cawangan = Column(String(255))
    alamat_cawangan = Column(Text)
    state = Column(String(50))
    processed_at = Column(DateTime, default=func.now())


# List of Malaysian states for detection
MALAYSIA_STATES = [
    'johor', 'kedah', 'kelantan', 'melaka', 'negeri sembilan',
    'pahang', 'perak', 'perlis', 'pulau pinang', 'sabah',
    'sarawak', 'selangor', 'terengganu', 'kuala lumpur',
    'labuan', 'putrajaya'
]


def is_header_row(row_cells):
    # Check if the row is a header row by looking for the expected column names
    if len(row_cells) < 4:
        return False

    expected_headers = ['bil.', 'rujukan ppm', 'nama cawangan', 'alamat cawangan']
    cell_texts = [cell.get('text', '').lower().strip() for cell in row_cells[:4]]

    matches = sum(1 for i, header in enumerate(expected_headers) if header in cell_texts[i])
    return matches >= 3


def is_state(text):
    """
    Checks if the given text contains a Malaysian state or federal territory name.
    Prioritizes longer, more specific matches.
    """
    text_lower = text.lower()

    # Sort states by length descending to prioritize longer, more specific matches
    sorted_states = sorted(MALAYSIA_STATES, key=len, reverse=True)

    for state in sorted_states:
        if re.search(r'\b' + re.escape(state) + r'\b', text_lower):
            # Special handling for "Wilayah Persekutuan" to title case it correctly if it's the full match
            if state == "wilayah persekutuan":
                return True, "Wilayah Persekutuan"
            elif state.startswith("wilayah persekutuan"):
                # Title case the full "Wilayah Persekutuan XXXX" like
                return True, state.title()
            else:
                return True, state.title()  # Title case for other states
    return False, None


def is_duplicate_in_branch_info(bil: str, s3_path: str, db: Session) -> bool:

    # Checks if a row with the given 'bil' and 's3_path' already exists in the BranchInfo table. Should be unique for each row to identify duplicates.
    existing = db.query(BranchInfo).filter_by(bil=bil, s3_path=s3_path).first()
    if existing:
        print(
            f"{Fore.CYAN}[DEBUG] ⚠️  Row (BIL={bil}, S3_PATH={s3_path}) already exists in BranchInfo.{Style.RESET_ALL}")
        return True
    return False


def is_already_flagged_for_review(bil: str, s3_path: str, page_number: int, db: Session) -> bool:

    # Checks if a row with the given 'bil', 's3_path', and 'page_number already exists in the PartialBranchInfoReview table.
    # Use page_number for more precise flagging for lines
    existing = db.query(PartialBranchInfoReview).filter_by(
        bil=bil, s3_path=s3_path, page_number=page_number
    ).first()
    if existing:
        print(
            f"{Fore.YELLOW}[DEBUG] 🟡 Row (BIL={bil}, S3_PATH={s3_path}, Page={page_number}) already flagged for review.{Style.RESET_ALL}")
        return True
    return False


def calculate_extraction_confidence(blocks, extracted_block_ids):

    # Calculates an average confidence score for the extracted table data. Focuses only on TEXTRACT Blocks that contributed to the final data rows.

    if not extracted_block_ids:
        total_text_confidence = 0.0
        count_text_blocks = 0
        for block in blocks:
            if block['BlockType'] in ['LINE', 'WORD'] and 'Confidence' in block:
                total_text_confidence += block['Confidence']
                count_text_blocks += 1
        return total_text_confidence / count_text_blocks if count_text_blocks > 0 else 0.0

    total_confidence = 0.0
    count_relevant_blocks = 0

    block_map = {block['Id']: block for block in blocks}

    for block_id in extracted_block_ids:
        block = block_map.get(block_id)
        if block and 'Confidence' in block:
            total_confidence += block['Confidence']
            count_relevant_blocks += 1

    return total_confidence / count_relevant_blocks if count_relevant_blocks > 0 else 0.0


def extract_table_data(blocks, s3_path, db):
    # Process Textract blocks to extract table data and calculate confidence.
    pages = {}
    block_map = {}
    for block in blocks:
        block_map[block['Id']] = block
        if block['BlockType'] == 'PAGE':
            page_num = block['Page']
            pages[page_num] = {'tables': [], 'cells': {}, 'lines': {}, 'words': {}}

    for block in blocks:
        page_num = block.get('Page')
        if not page_num or page_num not in pages:
            continue

        if block['BlockType'] == 'TABLE':
            pages[page_num]['tables'].append(block)
        elif block['BlockType'] == 'CELL':
            pages[page_num]['cells'][block['Id']] = block
        elif block['BlockType'] == 'LINE':
            pages[page_num]['lines'][block['Id']] = block
        elif block['BlockType'] == 'WORD':
            pages[page_num]['words'][block['Id']] = block

    all_data = []
    all_extracted_block_ids = set()
    problematic_rows = []
    successful_table_bils = set()
    flagged_for_review_cache = set()

    # --- PHASE 1: Extract data from successfully detected TABLES ---
    for page_num in sorted(pages.keys()):
        page = pages[page_num]

        page_blocks_sorted = []
        for line_id, line in page['lines'].items():
            page_blocks_sorted.append(line)
        for table_block in page['tables']:
            page_blocks_sorted.append(table_block)

        # Sort blocks by their Y-coordinate (Top) to respect reading order
        page_blocks_sorted.sort(key=lambda b: b['Geometry']['BoundingBox']['Top'])

        for block in page_blocks_sorted:
            if block['BlockType'] == 'LINE':
                continue
            elif block['BlockType'] == 'TABLE':
                table = block
                rows = {}
                if 'Relationships' not in table:
                    continue

                for rel in table['Relationships']:
                    if rel['Type'] == 'CHILD':
                        for cell_id in rel['Ids']:
                            if cell_id in page['cells']:
                                cell = page['cells'][cell_id]
                                all_extracted_block_ids.add(cell['Id'])
                                row_idx = cell['RowIndex']
                                col_idx = cell['ColumnIndex']

                                cell_text = ""
                                if 'Relationships' in cell:
                                    for cell_rel in cell['Relationships']:
                                        if cell_rel['Type'] == 'CHILD':
                                            for word_id in cell_rel['Ids']:
                                                word_block = page['words'].get(word_id) or page['lines'].get(word_id)
                                                if word_block and word_block['BlockType'] in ['WORD', 'LINE']:
                                                    cell_text += word_block['Text'] + " "
                                                    all_extracted_block_ids.add(word_block['Id'])

                                cell_text = cell_text.strip()

                                if row_idx not in rows:
                                    rows[row_idx] = {}
                                rows[row_idx][col_idx] = {'text': cell_text, 'row': row_idx, 'col': col_idx,
                                                          'confidence': cell.get('Confidence', 100.0)}

                sorted_rows = [rows[idx] for idx in sorted(rows.keys())]
                start_idx = 0
                if sorted_rows and is_header_row([sorted_rows[0].get(i, {'text': ''}) for i in range(1, 5)]):
                    start_idx = 1

                for i in range(start_idx, len(sorted_rows)):
                    row = sorted_rows[i]
                    if not row or 1 not in row:
                        continue

                    raw_column_1_text = row.get(1, {}).get('text', '')
                    raw_column_2_text = row.get(2, {}).get('text', '')
                    raw_column_3_text = row.get(3, {}).get('text', '')
                    raw_column_4_text = row.get(4, {}).get('text', '')

                    # TODO: remove these logs after testing, for debugging purposes only.
                    print(
                        f"{Fore.BLUE}[DEBUG] ℹ️  Processing row {row.get(1, {}).get('row')} on page {page_num}{Style.RESET_ALL}")
                    print(f"{Fore.BLUE}[DEBUG] ℹ️  raw_column_1_text: '{raw_column_1_text}'{Style.RESET_ALL}")
                    print(f"{Fore.BLUE}[DEBUG] ℹ️  raw_column_2_text: '{raw_column_2_text}'{Style.RESET_ALL}")
                    print(f"{Fore.BLUE}[DEBUG] ℹ️  raw_column_3_text: '{raw_column_3_text}'{Style.RESET_ALL}")
                    print(f"{Fore.BLUE}[DEBUG] ℹ️  raw_column_4_text: '{raw_column_4_text}'{Style.RESET_ALL}")

                    processed_first_cell = raw_column_1_text.strip()
                    bil = None
                    leftover_in_bil_col = ""
                    bil_match = re.match(r'^\s*(?:bil|no)?\s*\.?\s*(\d+)\.?\s*(.*)?$', processed_first_cell,
                                         flags=re.IGNORECASE)

                    if bil_match:
                        bil = bil_match.group(1)
                        leftover_in_bil_col = bil_match.group(2).strip()
                    else:
                        continue

                    if is_duplicate_in_branch_info(bil=bil, s3_path=s3_path, db=db):
                        continue

                    extracted_rujukan_ppm_candidate = raw_column_2_text.strip()
                    if not extracted_rujukan_ppm_candidate:
                        extracted_rujukan_ppm_candidate = leftover_in_bil_col

                    ppm_pattern = re.compile(r'(PPM/[A-Z0-9\s\-\./]+(?:[\s-]*\d+)?)', re.IGNORECASE)
                    ppm_match = ppm_pattern.search(extracted_rujukan_ppm_candidate)

                    if ppm_match:
                        parsed_rujukan_ppm = ppm_match.group(1).strip()
                    else:
                        parsed_rujukan_ppm = ""

                    parsed_nama_cawangan = raw_column_3_text.strip()
                    parsed_alamat_cawangan = raw_column_4_text.strip()

                    if not parsed_nama_cawangan and not parsed_alamat_cawangan and leftover_in_bil_col:
                        temp_leftover = leftover_in_bil_col.replace(parsed_rujukan_ppm, '').strip()
                        parts = re.split(r'\s+(?:-\s*)?', temp_leftover, 1)
                        parts = [p.strip() for p in parts if p.strip()]
                        if len(parts) >= 1:
                            parsed_nama_cawangan = parts[0]
                        if len(parts) >= 2:
                            parsed_alamat_cawangan = parts[1]

                    extracted_state_from_address = None
                    if parsed_alamat_cawangan:
                        _, state_from_address_temp = is_state(parsed_alamat_cawangan)
                        extracted_state_from_address = state_from_address_temp

                    final_row_state_for_output = extracted_state_from_address
                    reason_list = []

                    if not final_row_state_for_output:
                        reason_list.append('Failed to determine state from address')
                    if not parsed_rujukan_ppm:
                        reason_list.append('Failed to extract rujukan_ppm')
                    if not parsed_nama_cawangan:
                        reason_list.append('Failed to extract nama_cawangan')
                    if not parsed_alamat_cawangan:
                        reason_list.append('Failed to extract alamat_cawangan')

                    is_concatenated_row = False
                    if leftover_in_bil_col and (
                            not parsed_rujukan_ppm or not parsed_nama_cawangan or not parsed_alamat_cawangan):
                        is_concatenated_row = True

                    if is_concatenated_row or reason_list:
                        if is_already_flagged_for_review(bil=bil, s3_path=s3_path, page_number=page_num, db=db):
                            continue

                        if (bil, page_num) in flagged_for_review_cache:
                            continue

                        flag_reason = ", ".join(
                            reason_list) if not is_concatenated_row else "Concatenated Row, " + ",".join(reason_list)

                        problematic_rows.append({
                            'page_number': page_num,
                            'row_index': row.get(1, {}).get('row'),
                            'bil': bil,
                            'raw_column_1_text': raw_column_1_text,
                            'raw_column_2_text': raw_column_2_text,
                            'raw_column_3_text': raw_column_3_text,
                            'raw_column_4_text': raw_column_4_text,
                            'parsed_rujukan_ppm': parsed_rujukan_ppm,
                            'parsed_nama_cawangan': parsed_nama_cawangan,
                            'parsed_alamat_cawangan': parsed_alamat_cawangan,
                            'state': final_row_state_for_output,
                            'reason': flag_reason
                        })
                        flagged_for_review_cache.add((bil, page_num))
                        continue

                    all_data.append({
                        'bil': bil,
                        'rujukan_ppm': parsed_rujukan_ppm,
                        'nama_cawangan': parsed_nama_cawangan,
                        'alamat_cawangan': parsed_alamat_cawangan,
                        'state': final_row_state_for_output
                    })
                    successful_table_bils.add(bil)

    # --- PHASE 2: Secondary Pass - Fallback for orphan LINE blocks (Flag for review) ---
    orphan_line_blocks = []
    for pn in sorted(pages.keys()):
        page = pages[pn]
        for line_id, line in page['lines'].items():
            if line_id not in all_extracted_block_ids:
                orphan_line_blocks.append(line)

    orphan_line_blocks.sort(key=lambda b: (b.get('Page', 0), b['Geometry']['BoundingBox']['Top']))

    for line in orphan_line_blocks:
        line_text = line.get('Text', '').strip()
        page_num_orphan = line.get('Page', 0)

        line_match = re.match(
            r'^\s*(?:bil|no)?\s*\.?\s*(\d+)\.?\s*(PPM/[A-Z0-9\s\-\./]+(?:[\s-]*\d+)?)\s*(.*?)(?:\s+-\s*|(?=\s*(?:PPM|[A-Z]{2,}))|\s*$)(.*)$',
            line_text, flags=re.IGNORECASE
        )

        if line_match:
            bil = line_match.group(1)
            parsed_rujukan_ppm = line_match.group(2).strip()
            parsed_nama_cawangan = line_match.group(3).strip()
            parsed_alamat_cawangan = line_match.group(4).strip()

            if bil in successful_table_bils:
                continue

            if is_already_flagged_for_review(bil=bil, s3_path=s3_path, page_number=page_num_orphan, db=db):
                continue

            if (bil, page_num_orphan) in flagged_for_review_cache:
                continue

            extracted_state_from_address = None
            if parsed_alamat_cawangan:
                _, state_from_address_temp = is_state(parsed_alamat_cawangan)
                extracted_state_from_address = state_from_address_temp

            final_row_state = extracted_state_from_address
            reason_list = []

            if not final_row_state:
                reason_list.append('Failed to determine state from address')
            if not parsed_rujukan_ppm:
                reason_list.append('Failed to extract rujukan_ppm')
            if not parsed_nama_cawangan:
                reason_list.append('Failed to extract nama_cawangan')
            if not parsed_alamat_cawangan:
                reason_list.append('Failed to extract alamat_cawangan')

            flag_reason = "Failed to extract from table, " + ", ".join(reason_list)

            problematic_rows.append({
                'page_number': page_num_orphan,
                'row_index': None,
                'bil': bil,
                'raw_column_1_text': line_text,
                'raw_column_2_text': '',
                'raw_column_3_text': '',
                'raw_column_4_text': '',
                'parsed_rujukan_ppm': parsed_rujukan_ppm,
                'parsed_nama_cawangan': parsed_nama_cawangan,
                'parsed_alamat_cawangan': parsed_alamat_cawangan,
                'state': final_row_state,
                'reason': flag_reason
            })
            flagged_for_review_cache.add((bil, page_num_orphan))
            all_extracted_block_ids.add(line['Id'])

    overall_confidence = calculate_extraction_confidence(blocks, all_extracted_block_ids)

    # Determine document status flags
    has_problematic_rows = len(problematic_rows) > 0
    has_successful_rows = len(all_data) > 0
    is_partial = has_problematic_rows and has_successful_rows

    return all_data, overall_confidence, problematic_rows, is_partial, has_problematic_rows


def extract_table_from_pdf(bucket_name, document_key, db_session):
    """Extract tables from a PDF document in S3 using AWS Textract"""
    textract = boto3.client('textract', region_name='ap-southeast-1')

    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': document_key
            }
        },
        FeatureTypes=['TABLES']
    )

    job_id = response['JobId']
    print(f"{Fore.CYAN}📝 Textract job started! Job ID: {Style.BRIGHT}{job_id}{Style.NORMAL}{Style.RESET_ALL}")

    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response['JobStatus']

        if status in ['SUCCEEDED', 'FAILED']:
            break

        print(
            f"{Fore.YELLOW}⏳ Textract job status: {Style.BRIGHT}{status}{Style.NORMAL} – still processing...{Style.RESET_ALL}")
        time.sleep(5)

    if status == 'FAILED':
        print(f"{Fore.RED}❌ Textract job failed for {document_key}{Style.RESET_ALL}")
        return [], None, []

    all_blocks = []
    next_token = None

    while True:
        if next_token:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_analysis(JobId=job_id)

        all_blocks.extend(response['Blocks'])

        if 'NextToken' in response:
            next_token = response['NextToken']
        else:
            break

    # TODO: Save raw Textract output to a file, to remove after debugging
    output_filename = f"textract_raw_output_{document_key.replace('/', '_').replace('.pdf', '')}.json"
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(all_blocks, f, indent=2, ensure_ascii=False)
        print(
            f"{Fore.LIGHTBLACK_EX}📝 Raw Textract output for {document_key} saved to {output_filename}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}❌ Error saving raw Textract output to file: {e}{Style.RESET_ALL}")

    # Pass document_key (as s3_path) and the db_session to extract_table_data
    all_data, overall_confidence, problematic_rows, document_with_partial_rows_flagged, total_documents_with_problematic_rows = extract_table_data(all_blocks, document_key, db_session)
    return all_data, overall_confidence, problematic_rows, document_with_partial_rows_flagged, total_documents_with_problematic_rows


def save_to_database(data, db_connection_string, s3_path_param):
    """Save the extracted data to MySQL database"""
    engine = create_engine(db_connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for item in data:
            branch = BranchInfo(
                s3_path=s3_path_param,
                bil=item['bil'],
                rujukan_ppm=item['rujukan_ppm'],
                nama_cawangan=item['nama_cawangan'],
                alamat_cawangan=item['alamat_cawangan'],
                state=item['state']
            )
            session.add(branch)
        session.commit()
        print(
            f"{Fore.GREEN}{Style.BRIGHT}[SUCCESS] ✅ Successfully saved {len(data)} record{'s' if len(data) != 1 else ''} for {s3_path_param} to the '{BranchInfo.__tablename__} table'{Style.RESET_ALL}")
    except Exception as e:
        session.rollback()
        print(
            f"{Fore.RED}❌ Error saving to '{BranchInfo.__tablename__}' for {s3_path_param}: {str(e)}{Style.RESET_ALL}")
        raise
    finally:
        session.close()


def save_unprocessed_doc(s3_path, reason, confidence, db_connection_string):
    """Save a record of an unprocessed document to the 'unprocessed_docs' table."""
    engine = create_engine(db_connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        unprocessed = UnprocessedDoc(
            s3_path=s3_path,
            reason=reason,
            extraction_confidence=confidence
        )
        session.add(unprocessed)
        session.commit()
        print(
            f"{Fore.LIGHTRED_EX}{Style.BRIGHT}[CRITICAL] 🚨 Document {s3_path} flagged as unprocessed (Reason: {reason}, Confidence: {confidence:.2f}) - Require manual review and insert to avoid data loss.{Style.RESET_ALL}")
    except Exception as e:
        session.rollback()
        if 'Duplicate entry' in str(e) or 'UNIQUE constraint failed' in str(e):
            print(
                f"{Fore.LIGHTBLACK_EX}ℹ️  Document {s3_path} already exists in '{UnprocessedDoc.__tablename__}' table.{Style.RESET_ALL}")
        else:
            print(
                f"{Fore.RED}❌ Error saving to '{UnprocessedDoc.__tablename__}' for {s3_path}: {str(e)}{Style.RESET_ALL}")
    finally:
        session.close()


def save_problematic_rows(problem_data, db_connection_string, s3_path_param):
    engine = create_engine(db_connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for item in problem_data:
            review_item = PartialBranchInfoReview(
                s3_path=s3_path_param,
                page_number=item['page_number'],
                row_index=item['row_index'],
                bil=item['bil'],
                raw_column_1_text=item['raw_column_1_text'],
                raw_column_2_text=item['raw_column_2_text'],
                raw_column_3_text=item['raw_column_3_text'],
                raw_column_4_text=item['raw_column_4_text'],
                parsed_rujukan_ppm=item['parsed_rujukan_ppm'],
                parsed_nama_cawangan=item['parsed_nama_cawangan'],
                parsed_alamat_cawangan=item['parsed_alamat_cawangan'],
                state=item['state'],
                reason=item['reason']
            )
            session.add(review_item)
        session.commit()
        # Access the table name dynamically using the __tablename__ attribute
        table_name = PartialBranchInfoReview.__tablename__
        print(
            f"{Fore.RED}{Style.BRIGHT}[WARNING] 🔍 Flagged {len(problem_data)} problematic row{'s' if len(problem_data) != 1 else ''} for {s3_path_param} for manual review at table '{table_name}'.{Style.RESET_ALL}")
    except Exception as e:
        session.rollback()
        print(f"{Fore.RED}❌ Error saving problematic rows for {s3_path_param}: {str(e)}{Style.RESET_ALL}")
    finally:
        session.close()


def get_all_kmc_paths(bucket_name, min_year=2015):
    """Get all S3 paths that contain KMC directories under any party's Cawangan folder"""
    s3_client = boto3.client('s3')
    kmc_paths = []
    party_with_valid_kmc_data = []

    # First list all parties in the base directory
    parties = set()
    paginator = s3_client.get_paginator('list_objects_v2')

    # List top-level party directories
    for page in paginator.paginate(Bucket=bucket_name, Prefix="Parti_Politik-Induk_Modified/", Delimiter='/'):
        for prefix in page.get('CommonPrefixes', []):
            party_path = prefix['Prefix']
            party_name = party_path.split('/')[-2]
            if party_name:
                parties.add(party_name)

    print(f"{Fore.CYAN}Found {len(parties)} political parties to check for Cawangan/KMC directories{Style.RESET_ALL}")

    # For each party, check if it has Cawangan/KMC structure
    for party in parties:
        cawangan_path = f"Parti_Politik-Induk_Modified/{party}/Cawangan/"

        # Check if Cawangan directory exists
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=cawangan_path,
            MaxKeys=1
        )

        if 'Contents' in response or 'CommonPrefixes' in response:
            # Now look for KMC directories under Cawangan
            kmc_prefix = f"{cawangan_path}KMC/"
            kmc_response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=kmc_prefix,
                Delimiter='/'
            )

            # Check for year folders under KMC
            if 'CommonPrefixes' in kmc_response:
                party_with_valid_kmc_data.append(party)
                for year_prefix in kmc_response['CommonPrefixes']:
                    year_path = year_prefix['Prefix']
                    try:
                        # Extract year from path (should be the last part before trailing slash)
                        year = int(year_path.split('/')[-2])
                        if year >= min_year:
                            kmc_paths.append(year_path)
                            print(f"{Fore.LIGHTMAGENTA_EX}☑️ Found KMC directory for party: {party} (Year: {year}){Style.RESET_ALL}")
                        else:
                            print(
                                f"{Fore.LIGHTBLACK_EX}⏭️ Skipping {year_path} (Year {year} < {min_year}){Style.RESET_ALL}")
                    except ValueError:
                        print(f"{Fore.YELLOW}Skipping non-year folder: {year_path}{Style.RESET_ALL}")
            else:
                print(f"{Fore.YELLOW}🟡 Party {party} has KMC but no year directories{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}{Style.DIM}❌ Party {party} has no Cawangan directory{Style.RESET_ALL}")

    print(f"{Fore.CYAN}{Style.BRIGHT}📊 Parties with valid KMC data: {len(party_with_valid_kmc_data)}{Style.RESET_ALL}\n")
    return kmc_paths


def print_party_summary(party_name, stats):
    """Print a formatted summary for a single party"""
    header = f"║   PARTY SUMMARY: {party_name}   ║"
    top_separator = "╔" + "═" * (len(header)-2) + "╗"
    bottom_separator = "╚" + "═" * (len(header)-2) + "╝"

    print(f"\n{Fore.CYAN}{top_separator}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{header}")
    print(f"{Fore.CYAN}{bottom_separator}{Style.RESET_ALL}\n")

    # Documents Processed Section
    print(f"{Fore.MAGENTA}{Style.BRIGHT}📑 Documents Processed:{Style.RESET_ALL}")
    print(f"  {Fore.LIGHTWHITE_EX}📋 Total KMC for {party_name}: {stats['documents_processed']}{Style.RESET_ALL}")

    if stats['documents_processed'] > 0:
        fully_successful_pct = (stats['documents_fully_successful'] / stats['documents_processed']) * 100
        partial_success_pct = (stats['documents_partially_successful'] / stats['documents_processed']) * 100
        failed_pct = (stats['documents_failed'] / stats['documents_processed']) * 100

        print(f"    {Fore.GREEN}{Style.DIM}✅ Fully Successful (All rows extracted): {stats['documents_fully_successful']} ({fully_successful_pct:.2f}%){Style.RESET_ALL}")
        print(f"    {Fore.YELLOW}{Style.DIM}🔶 Partially Successful (Some rows flagged): {stats['documents_partially_successful']} ({partial_success_pct:.2f}%){Style.RESET_ALL}")
        print(f"    {Fore.RED}{Style.DIM}❌ Failed (Flagged as unprocessed):{Fore.RED} {stats['documents_failed']} ({failed_pct:.2f}%){Style.RESET_ALL}")
    else:
        print(f"  {Fore.LIGHTRED_EX}🚨 No documents were processed{Style.RESET_ALL}")

    # Data Extracted Section
    print(f"\n{Fore.MAGENTA}{Style.BRIGHT}📊 Data Extracted:{Style.RESET_ALL}")
    print(f"    {Fore.GREEN}{Style.DIM}💾 Rows Extracted and Saved: {stats['rows_extracted']}{Style.RESET_ALL}")
    print(f"    {Fore.MAGENTA}{Style.DIM}🔍 Problematic Rows Flagged: {stats['problematic_rows']}{Style.RESET_ALL}")

    if stats['rows_extracted'] > 0:
        success_rate = (stats['rows_extracted'] / (stats['rows_extracted'] + stats['problematic_rows'])) * 100
        print(f"    {Fore.LIGHTWHITE_EX}{Style.DIM}💎 Data Quality Rate:{Style.RESET_ALL} {Fore.GREEN if success_rate >= 80 else Fore.LIGHTGREEN_EX if success_rate >= 70 else Fore.YELLOW if success_rate >= 50 else Fore.RED}{success_rate:.2f}%{Style.RESET_ALL}")
    else:
        print(f"    {Fore.YELLOW}⚠️ No rows were extracted{Style.RESET_ALL}")

    if stats['documents_processed'] > 0:
        overall_success = ((stats['documents_fully_successful'] + stats['documents_partially_successful']) /
                          stats['documents_processed']) * 100
        success_color = Fore.GREEN if overall_success >= 80 else Fore.LIGHTGREEN_EX if overall_success >= 80 else Fore.YELLOW if overall_success >= 50 else Fore.RED
        print(f"\n{Fore.LIGHTWHITE_EX}🎯 Document Processing Success Rate (Fully + Partially Successful): {Style.RESET_ALL}{success_color}{Style.BRIGHT}{overall_success:.2f}%{Style.RESET_ALL}")
    else:
        print(f"    {Fore.YELLOW}⚠️ No documents were extracted{Style.RESET_ALL}")


def main():
    BUCKET_NAME = "induk-account-training"
    DB_CONNECTION_STRING = "mysql+pymysql://Jason:Jwsk737373%21@localhost:3306/eroses2"
    MIN_YEAR = 2015  # Here I set to 2015, based on the PM's decision

    LOW_CONFIDENCE_THRESHOLD = 80.0
    HIGH_CONFIDENCE_NO_DATA_THRESHOLD = 85.0

    if not all([BUCKET_NAME, DB_CONNECTION_STRING]):
        print(f"{Fore.RED}❌ Missing required environment variables. Please check your .env file.{Style.RESET_ALL}")
        return

    # Get all KMC paths across all parties
    kmc_paths = get_all_kmc_paths(BUCKET_NAME, min_year=MIN_YEAR)

    if not kmc_paths:
        print(f"{Fore.RED}❌ No KMC directories found in any party folders{Style.RESET_ALL}")
        return

    # Sort KMC paths by party name to group them together
    kmc_paths.sort(key=lambda x: x.split('/')[1])

    from collections import defaultdict
    party_kmc_paths = defaultdict(list)
    for path in kmc_paths:
        party_name = path.split('/')[1]
        party_kmc_paths[party_name].append(path)

    s3_client = boto3.client('s3')
    engine = create_engine(DB_CONNECTION_STRING)
    Base.metadata.create_all(engine)
    Session_factory = sessionmaker(bind=engine)

    # Counters for my logs
    total_extracted_rows = 0
    total_documents_processed_fully_successful = 0
    total_documents_skipped = 0
    total_documents_flagged_unprocessed = 0
    total_documents_with_problematic_rows = 0
    total_documents_with_partial_rows_flagged = 0
    total_problematic_rows_cumulative = 0
    total_documents_found = 0

    # Dictionary to store per-party statistics
    party_stats = {}
    current_party = None
    party_kmc_counter = 0  # Counter for current party's KMC directories
    total_kmc_for_party = 0  # Total KMC directories for current party

    with Session_factory() as db_session_checker:
        try:
            existing_fully_processed_records = db_session_checker.query(BranchInfo.s3_path).distinct().all()
            all_in_branch_info = {record[0] for record in existing_fully_processed_records}

            existing_problematic_review_records = db_session_checker.query(
                PartialBranchInfoReview.s3_path).distinct().all()
            flagged_for_review_keys = {record[0] for record in existing_problematic_review_records}

            processed_keys = all_in_branch_info - flagged_for_review_keys
            print(
                f"{Fore.MAGENTA}📊 Found {Style.BRIGHT}{len(processed_keys)}{Style.NORMAL} document{'s' if len(processed_keys) != 1 else ''} previously processed successfully into the '{BranchInfo.__tablename__}' table.{Style.RESET_ALL}")
            print(
                f"{Fore.MAGENTA}📊 Found {Style.BRIGHT}{len(flagged_for_review_keys)}{Style.NORMAL} document{'s' if len(processed_keys) != 1 else ''} with problematic rows previously flagged into the '{PartialBranchInfoReview.__tablename__}' table for review.{Style.RESET_ALL}")

            existing_unprocessed_records = db_session_checker.query(UnprocessedDoc.s3_path).distinct().all()
            unprocessed_keys = {record[0] for record in existing_unprocessed_records}
            print(
                f"{Fore.MAGENTA}📊 Found {Style.BRIGHT}{len(unprocessed_keys)}{Style.NORMAL} document{'s' if len(processed_keys) != 1 else ''} previously flagged as unprocessed in the '{UnprocessedDoc.__tablename__}' table.{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Could not retrieve previously processed document keys: {e}{Style.RESET_ALL}")

    # Process each KMC directory found
    for kmc_path in kmc_paths:
        party_name = kmc_path.split('/')[1]

        if party_name != current_party:
            # If we were processing a previous party, print its summary now
            if current_party is not None:
                print_party_summary(current_party, party_stats[current_party])
                print(f"\n{Fore.CYAN}{Style.BRIGHT}=== Finished processing {current_party} ==={Style.RESET_ALL}")

            # Initialize new party stats and reset counters
            current_party = party_name
            party_kmc_counter = 0
            total_kmc_for_party = len([p for p in kmc_paths if p.split('/')[1] == party_name])

            if current_party not in party_stats:
                party_stats[current_party] = {
                    'documents_processed': 0,
                    'rows_extracted': 0,
                    'problematic_rows': 0,
                    'documents_fully_successful': 0,
                    'documents_partially_successful': 0,
                    'documents_failed': 0
                }

            print(
                f"\n{Fore.WHITE}{Style.BRIGHT}=== Starting processing for party: {current_party} ==={Style.RESET_ALL}\n")
            print(
                f"{Fore.LIGHTBLUE_EX}📂 Found {total_kmc_for_party} KMC director{'ies' if total_kmc_for_party > 1 else 'y'} for {current_party}{Style.RESET_ALL}")

        # Increment and display KMC directory counter for current party
        party_kmc_counter += 1
        print(
            f"\n{Fore.LIGHTBLUE_EX}📂 Processing {current_party}'s KMC directory {party_kmc_counter}/{total_kmc_for_party}: {kmc_path}{Style.RESET_ALL}")

        # Get all PDFs in this KMC directory
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=kmc_path)

        documents_to_process = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].lower().endswith('.pdf'):
                        documents_to_process.append(obj["Key"])

        if not documents_to_process:
            print(f"{Fore.YELLOW}⚠️  No PDF documents found in s3://{BUCKET_NAME}/{kmc_path}{Style.RESET_ALL}")
            continue

        total_documents_found += len(documents_to_process)
        party_stats[party_name]['documents_processed'] += len(documents_to_process)
        document_progress_counter = 0

        for doc_key in documents_to_process:
            document_progress_counter += 1
            print(
                f"\n{Fore.CYAN}--- Processing Document {document_progress_counter}/{len(documents_to_process)}: {doc_key} ---{Style.RESET_ALL}")

            if doc_key in processed_keys:
                print(f"{Fore.GREEN}✅ Document {doc_key} already fully processed. Skipping.{Style.RESET_ALL}")
                total_documents_skipped += 1
                continue
            if doc_key in unprocessed_keys:
                print(
                    f"{Fore.YELLOW}⚠️  Document {doc_key} previously flagged as unprocessed. Skipping.{Style.RESET_ALL}")
                total_documents_skipped += 1
                continue
            if doc_key in flagged_for_review_keys:
                print(
                    f"{Fore.YELLOW}⚠️  Document {doc_key} previously flagged with problematic rows. Skipping.{Style.RESET_ALL}")
                total_documents_skipped += 1
                continue

            try:
                with Session_factory() as db_session:
                    extracted_data, confidence, problematic_rows, is_partial, has_problematic_rows = extract_table_from_pdf(
                        BUCKET_NAME, doc_key, db_session)

                    # Update party stats
                    party_stats[party_name]['rows_extracted'] += len(extracted_data)
                    party_stats[party_name]['problematic_rows'] += len(problematic_rows)

                    # --- Confidence check first ---
                    if confidence is not None and confidence < LOW_CONFIDENCE_THRESHOLD:
                        reason = f"Overall extraction confidence ({confidence:.2f}%) below threshold ({LOW_CONFIDENCE_THRESHOLD}%)"
                        save_unprocessed_doc(doc_key, reason, confidence, DB_CONNECTION_STRING)
                        total_documents_flagged_unprocessed += 1
                        party_stats[party_name]['documents_failed'] += 1
                        unprocessed_keys.add(doc_key)
                        continue

                    # --- Handle documents with data ---
                    if extracted_data:
                        save_to_database(extracted_data, DB_CONNECTION_STRING, doc_key)
                        total_extracted_rows += len(extracted_data)

                        # Document is considered fully successful ONLY if:
                        # 1. We have extracted data
                        # 2. There are NO problematic rows
                        if not problematic_rows:
                            total_documents_processed_fully_successful += 1
                            party_stats[party_name]['documents_fully_successful'] += 1
                            processed_keys.add(doc_key)
                        else:
                            save_problematic_rows(problematic_rows, DB_CONNECTION_STRING, doc_key)
                            total_problematic_rows_cumulative += len(problematic_rows)
                            flagged_for_review_keys.add(doc_key)
                            total_documents_with_problematic_rows += 1
                            party_stats[party_name]['documents_partially_successful'] += 1

                    else:  # No extracted data
                        if problematic_rows:
                            save_problematic_rows(problematic_rows, DB_CONNECTION_STRING, doc_key)
                            total_problematic_rows_cumulative += len(problematic_rows)
                            flagged_for_review_keys.add(doc_key)
                            total_documents_with_problematic_rows += 1

                        reason = "No data extracted."
                        if confidence is not None and confidence >= HIGH_CONFIDENCE_NO_DATA_THRESHOLD:
                            reason = "High confidence but every rows in the document has one or more columns that failed to extract (possibly concatenation error, columns are too close together, table structure may be too irregular)"

                        save_unprocessed_doc(doc_key, reason, confidence, DB_CONNECTION_STRING)
                        total_documents_flagged_unprocessed += 1
                        party_stats[party_name]['documents_failed'] += 1
                        unprocessed_keys.add(doc_key)

            except Exception as e:
                print(f"{Fore.RED}❌ Failed to process {doc_key}: {e}{Style.RESET_ALL}")
                save_unprocessed_doc(doc_key, f"Processing failed: {e}", 0.0, DB_CONNECTION_STRING)
                total_documents_flagged_unprocessed += 1
                party_stats[party_name]['documents_failed'] += 1
                unprocessed_keys.add(doc_key)

    # Print summary for the last party processed
    if current_party is not None:
        print_party_summary(current_party, party_stats[current_party])
        print(f"\n{Fore.CYAN}{Style.BRIGHT}=== Finished processing {current_party} ==={Style.RESET_ALL}")

    # Print final summary
    print(f"""{Fore.LIGHTCYAN_EX}{Style.BRIGHT}
    ╔══════════════════════════════════════════════╗
    ║           FINAL PROCESSING SUMMARY           ║
    ╚══════════════════════════════════════════════╝
    {Style.RESET_ALL}""")

    print(f"📄 Total documents found: {Style.BRIGHT}{total_documents_found}{Style.RESET_ALL}")

    if total_documents_found > 0:
        # Docs that were fully successful (no problematic rows, data saved)
        percent_fully_successful = (
                total_documents_processed_fully_successful / total_documents_found * 100) if total_documents_found > 0 else 0
        print(
            f"{Fore.GREEN}{Style.BRIGHT}✅ Documents processed successfully (All rows extracted): {Style.BRIGHT}{total_documents_processed_fully_successful} ({percent_fully_successful:.2f}%){Style.RESET_ALL}")

        # Docs that were skipped (already in DB)
        percent_skipped = (total_documents_skipped / total_documents_found * 100) if total_documents_found > 0 else 0
        print(
            f"{Fore.LIGHTBLACK_EX}⏩ Documents skipped (previously processed/flagged): {Style.BRIGHT}{total_documents_skipped} ({percent_skipped:.2f}%){Style.RESET_ALL}")

        # Docs that were flagged as unprocessed (e.g., Textract failed, low confidence, no data)
        percent_flagged_unprocessed = (
                total_documents_flagged_unprocessed / total_documents_found * 100) if total_documents_found > 0 else 0
        print(
            f"{Fore.LIGHTRED_EX}{Style.BRIGHT}🚨 Documents flagged as unprocessed (Low confidence, table extraction failed): {total_documents_flagged_unprocessed} ({percent_flagged_unprocessed:.2f}%){Style.RESET_ALL}")

        # Docs that had *any* problematic rows (this is the broader category)
        percent_problematic_review = (
                total_documents_with_problematic_rows / total_documents_found * 100) if total_documents_found > 0 else 0
        print(
            f"{Fore.RED}⚠️ Documents with ANY rows flagged for manual review: {Style.BRIGHT}{total_documents_with_problematic_rows} ({percent_problematic_review:.2f}%){Style.RESET_ALL}")

        # Docs that had *some* good data AND *some* problematic rows (the partial category)
        percent_partial_flagged = (
                total_documents_with_partial_rows_flagged / total_documents_found * 100) if total_documents_found > 0 else 0
        print(
            f"{Fore.YELLOW}{Style.BRIGHT}🔶 Documents with PARTIALLY flagged rows (mixed success/problematic): {Style.BRIGHT}{total_documents_with_partial_rows_flagged} ({percent_partial_flagged:.2f}%){Style.RESET_ALL}")

    else:
        print(f"{Fore.RED}❌ No documents were found or processed.{Style.RESET_ALL}")

    print(
        f"{Fore.MAGENTA}{Style.BRIGHT}🔍 Total problematic rows flagged: {Style.BRIGHT}{total_problematic_rows_cumulative}{Style.RESET_ALL}")
    print(
        f"{Fore.GREEN}💾 Total rows extracted and saved to '{BranchInfo.__tablename__}': {Style.BRIGHT}{total_extracted_rows}{Style.RESET_ALL}")

if __name__ == '__main__':
    main()