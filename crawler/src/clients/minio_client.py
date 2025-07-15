import io
import logging

from minio import Minio, S3Error

logger = logging.getLogger(__name__)


class MinioClient:
    def __init__(
        self,
        endpoint,
        access_key,
        secret_key,
        secure=False,
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    async def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        metadata: dict,
        content_type: str = "application/octet-stream",
    ) -> bool:
        try:
            # 버킷이 없을 경우 생성
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)

            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=io.BytesIO(data),
                metadata=metadata,
                length=len(data),
                content_type=content_type,
            )
            logger.info(
                f"File '{object_name}' uploaded to bucket '{bucket_name}' successfully."
            )
            return True
        except S3Error as e:
            logger.error(f"S3 Error during MinIO upload: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during MinIO upload: {e}")
            return False

    async def download_file(
        self,
        bucket_name: str,
        object_name: str,
    ) -> tuple[bytes, dict]:
        try:
            res = self.client.get_object(bucket_name, object_name)
            content = res.read()
            # 사용자 정의 메타데이터 재정의
            retrieved_metadata = {}
            for key, value in res.headers.items():
                if key.lower().startswith("x-amz-meta-"):
                    original_key = key[len("x-amz-meta-") :]
                    retrieved_metadata[original_key] = value
            res.close()
            res.release_conn()
            logger.info(
                f"File '{object_name}' downloaded from bucket '{bucket_name}' successfully."
            )
            return content, retrieved_metadata
        except S3Error as e:
            logger.error(f"S3 Error during MinIO download: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during MinIO download: {e}")
            raise
