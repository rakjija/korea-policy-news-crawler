import logging
from datetime import datetime

from clients.minio_client import MinioClient

logger = logging.getLogger(__name__)


async def load_raws_to_minio(
    minio_endpoint: str,
    minio_bucket_name: str,
    minio_access_key: str,
    minio_secret_key: str,
    scraped_raw_data: list[tuple[bytes, str, int]],
):
    """
    스크랩된 Raw HTML 데이터를 MinIO(Data Lake)에 저장합니다.
    """
    # MinIO 클라이언트 초기화
    minio_client = MinioClient(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    # Raw HTML -> MinIO
    successfully_uploaded_count = 0
    minio_uploaded_objects = []
    for (
        raw_html_content,
        original_url,
        news_id,
    ) in scraped_raw_data:
        # 현재 날짜 추출
        now = datetime.now()

        object_name = f"{now.year}/{now.month:02d}/{now.day:02d}/{news_id}_{now.strftime('%H%M%S')}.html"
        metadata = {
            "crawled_at": now.isoformat(),
        }
        is_success = await minio_client.upload_file(
            bucket_name=minio_bucket_name,
            object_name=object_name,
            data=raw_html_content,
            metadata=metadata,
            content_type="text/html",
        )
        if is_success:
            successfully_uploaded_count += 1
            minio_uploaded_objects.append(
                {
                    "news_id": news_id,
                    "minio_path": object_name,
                    "original_url": original_url,
                }
            )
        else:
            logger.error(f"Failed to upload raw HTML for news ID {news_id} to MinIO.")
    logger.info(
        f"Finished processing. Successfully uploaded {successfully_uploaded_count} raw HTML files to MinIO."
    )

    return minio_uploaded_objects
