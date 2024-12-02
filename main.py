import asyncio
import os
from contextlib import asynccontextmanager
from enum import verify

from aiobotocore.session import get_session
from dotenv import load_dotenv

load_dotenv()


class S3Client:
    def __init__(self,
                 access_key: str,
                 secret_key: str,
                 endpoint_url: str,
                 bucket_name: str,
                 ):
        self.config = {
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
            'endpoint_url': endpoint_url,
        }
        self.bucket_name = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client('s3', **self.config, verify=False) as client:
            yield client

    async def upload_file(self,
                          file_path: str,
                          ):
        object_name = os.path.basename(file_path)
        async with self.get_client() as client:
            with open(file_path, 'rb') as file:
                await client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_name,
                    Body=file,
                )


async def main():
    endpoint_url = 'https://s3.storage.selcloud.ru'
    bucket_name = 'test-public-bucket1'
    s3_client = S3Client(
        access_key=os.environ['ACCESS_KEY'],
        secret_key=os.environ['SECRET_KEY'],
        endpoint_url=endpoint_url,
        bucket_name=bucket_name,
    )

    await s3_client.upload_file('rabbitmq.png')


if __name__ == '__main__':
    asyncio.run(main())
