import asyncio
import logging
from datetime import datetime

from clients.minio_client import MinioClient
from models.news import News
from pipelines.transformed.raw_transformer import transform_raw_html

logger = logging.getLogger(__name__)


async def extract_raws_from_minio(
    minio_client: MinioClient,
    minio_bucket_name: str,
    minio_objects_to_extract: list[dict],
    batch_size: int = 5,
    delay_between_batches: int = 1,
) -> list[
    tuple[bytes, dict, dict]
]:  # (raw_html_content, metadata, original_object_info)
    """
    MinIO에 저장된 Raw HTML 파일을 다운로드합니다.
    """
    logger.info("Starting extraction of raw HTML from MinIO.")

    downloaded_data = []

    for i in range(0, len(minio_objects_to_extract), batch_size):
        batch_objects = minio_objects_to_extract[i : i + batch_size]
        logger.info(
            f"Processing extraction batch {i // batch_size + 1}: {len(batch_objects)} objects."
        )

        tasks = []
        for obj in batch_objects:
            object_name = obj["minio_path"]
            tasks.append(minio_client.download_file(minio_bucket_name, object_name))

        downloaded_results: list[
            tuple[bytes, dict] | BaseException
        ] = await asyncio.gather(*tasks, return_exceptions=True)

        for j, result in enumerate(downloaded_results):
            obj = batch_objects[j]  # original_object_info
            news_id = obj["news_id"]

            if isinstance(result, BaseException):
                logger.error(
                    f"Error downloading raw HTML for news ID {news_id} from MinIO: {result}"
                )
            else:
                raw_html_content, metadata = result
                downloaded_data.append(
                    (raw_html_content, metadata, obj)
                )  # 원본 객체 정보도 함께 반환

        if i + batch_size < len(minio_objects_to_extract):
            logger.info(
                f"Waiting for {delay_between_batches} seconds until next extraction batch..."
            )
            await asyncio.sleep(delay_between_batches)

    logger.info(
        f"Finished extraction of {len(downloaded_data)} raw HTML files from MinIO."
    )
    return downloaded_data


async def transform_downloaded_html(
    downloaded_raw_data: list[
        tuple[bytes, dict, dict]
    ],  # (raw_html_content, metadata, original_object_info)
) -> list[News]:
    """
    다운로드된 Raw HTML 데이터를 News 객체로 변환합니다.
    """
    logger.info("Starting transformation of downloaded HTML.")

    transformed_news_items = []

    for raw_html_content, metadata, original_object_info in downloaded_raw_data:
        news_id = original_object_info["news_id"]
        original_url = original_object_info.get("original_url", "")
        crawled_at_str = metadata.get("crawled_at")

        crawled_at_from_minio = None
        if crawled_at_str:
            try:
                crawled_at_from_minio = datetime.fromisoformat(crawled_at_str)
            except ValueError:
                logger.warning(
                    f"Invalid crawled_at metadata for news ID {news_id}. Using current time."
                )
                crawled_at_from_minio = datetime.now()
        else:
            logger.warning(
                f"crawled_at metadata not found for news ID {news_id}. Using current time."
            )
            crawled_at_from_minio = datetime.now()  # fallback

        try:
            news_item: News = transform_raw_html(
                raw_html_content=raw_html_content,
                original_url=original_url,
                crawled_at=crawled_at_from_minio,
            )
            transformed_news_items.append(news_item)
        except Exception as e:
            logger.error(f"Error transforming news ID {news_id}: {e}")

    logger.info(
        f"Finished transformation of {len(transformed_news_items)} news articles."
    )
    return transformed_news_items
