import logging
from datetime import datetime

from clients.minio_client import MinioClient
from pipelines.raw.raw_scraper import scrap_raw_html_batch

logger = logging.getLogger(__name__)


async def load_to_dl(
    news_urls: list[str],
    batch_size: int = 5,
    delay_between_batches: int = 2,
):
    # MinIO 클라이언트 초기화
    minio_client = MinioClient(
        endpoint="localhost:9000",  # TODO: 환경 변수화
        access_key="ROOTNAME",  # TODO: 환경 변수화
        secret_key="CHANGEME123",  # TODO: 환경 변수화
        secure=False,
    )
    minio_bucket_name = "raw-news"  # TODO: 환경 변수화

    # Raw HTML 배치 스크랩
    scraped_raw_data = await scrap_raw_html_batch(
        news_urls,
        batch_size,
        delay_between_batches,
    )

    # Raw HTML -> MinIO
    successfully_uploaded_count = 0
    minio_uploaded_objects = []
    for raw_html_content, original_url, news_id in scraped_raw_data:
        # 현재 날짜 추출
        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day

        object_name = (
            f"{year}/{month:02d}/{day:02d}/{news_id}_{now.strftime('%H%M%S')}.html"
        )
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
