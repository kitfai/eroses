
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import boto3
from typing import List
from logging.handlers import RotatingFileHandler
from botocore.exceptions import ClientError
Base = declarative_base()

AWS_REGION = 'ap-southeast-1'

class PartyName(Base):
    __tablename__ = 'party_name'

    id = Column(Integer, primary_key=True)
    s3_party_name = Column(String(150),nullable=False)
    party_name = Column(String(150),nullable=True)
    created_date = Column(DateTime, default=datetime.now)
    modified_date = Column(DateTime, default=datetime.now)


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

class PartyDirectoryProcessor:
    def __init__(self, bucket_name: str, root_directory: str):
        self.navigator = S3DirectoryNavigator(bucket_name)
        self.root_directory = root_directory.rstrip('/')
        self.bucket_name = bucket_name

def query_party_name_exist(session,s3_party_name):
    try:
        # Query the database for the party name
        result = session.query(PartyName).filter_by(s3_party_name=s3_party_name).first()

        if result:
            # If a result is found, return the party name
            return result.s3_party_name
        else:
            # If no result is found, return None or handle accordingly
            return None

    except Exception as e:
        # Handle any exceptions that occur during the query
        print(f"An error occurred: {e}")
        return None

def process_parties(bucket_name:str ,base_path:str)->List[str]:
    s3Lister = PartyDirectoryProcessor(bucket_name, base_path)
    parties_directories = s3Lister.navigator.list_directories(f"{base_path}/")  # ['parties']
    return  parties_directories
    # Process each directory

def save_party_name(session, s3_party_name, party_name=None):
    try:
        # Check if the party name already exists


        new_party = PartyName(s3_party_name=s3_party_name, party_name=party_name)
        session.add(new_party)

        # Commit the changes to the database
        session.commit()

    except Exception as e:
        # Handle any exceptions that occur during the save operation
        print(f"An error occurred: {e}")
        session.rollback()

def main():
    print('main')
    sql_connection = "mysql+mysqlconnector://root:strong_password@127.0.0.1:3307/eroses_dev"
    engine = create_engine(sql_connection)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()

    try:
        bucket_name = 'induk-account-training'
        root_directory = 'Parti_Politik-Induk_Modified'
        parties = process_parties(bucket_name, root_directory)
        # Query the database for the party name
        for party in parties:
            result = query_party_name_exist(session, party)
            print(f'part name {result}')
            if result:
                # If a result is found, return the party name
                continue
            else:
                # If no result is found, return None or handle accordingly
                save_party_name(session, party)

    except Exception as e:
        # Handle any exceptions that occur during the query
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the session
        session.close()




if __name__ == "__main__":
    main()