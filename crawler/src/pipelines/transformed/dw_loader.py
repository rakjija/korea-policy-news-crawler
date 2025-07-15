import asyncio
import logging
from datetime import datetime

from clients.minio_client import MinioClient
from clients.postgres_client import PostgresClient
from models.news import News
from pipelines.transformed.raw_transformer import transform_raw_html

logger = logging.getLogger(__name__)


async def load_to_dw(
    minio_client: MinioClient,
    pg_client: PostgresClient,
    minio_bucket_name: str,
    minio_objects_to_transform: list[dict],
    batch_size: int = 5,
    delay_between_batches: int = 1,
):
    """
    MinIO에 저장된 Raw HTML 파일을 다운로드하고,
    데이터 정제 및 News 객체로 변환 후
    PostgreSQL(DW)에 저장합니다.
    """
    logger.info("Starting loading raw data from MinIO to Data Warehouse.")

    successfully_transformed_count = 0

    # MinIO 객체들을 배치 단위로 처리
    for i in range(0, len(minio_objects_to_transform), batch_size):
        batch_objects = minio_objects_to_transform[i : i + batch_size]
        logger.info(
            f"Processing DW batch {i // batch_size + 1}: {len(batch_objects)} objects."
        )

        tasks = []
        for obj in batch_objects:
            news_id = obj["news_id"]
            object_name = obj["minio_path"]

            # MinIO에서 Raw HTML 다운로드
            tasks.append(minio_client.download_file(minio_bucket_name, object_name))

        downloaded_results: list[
            tuple[bytes, dict] | BaseException
        ] = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(downloaded_results):
            obj = batch_objects[i]
            news_id = obj["news_id"]
            original_url = obj.get("original_url", "")

            if isinstance(result, BaseException):
                logger.error(
                    f"Error downloading raw HTML for news ID {news_id} from MinIO: {result}"
                )
            else:
                raw_html_content, metadata = result
                crawled_at_str = metadata.get("crawled_at")
                if crawled_at_str:
                    crawled_at_from_minio = datetime.fromisoformat(crawled_at_str)
                else:
                    logger.warning(
                        f"crawled_at metadata not found for news ID {news_id}. Using current time."
                    )
                    crawled_at_from_minio = datetime.now()  # fallback
                try:
                    # Raw HTML을 News 객체로 변환
                    # crawled_at은 MinIO 메타데이터에서 가져옴
                    news_item: News = transform_raw_html(
                        raw_html_content=raw_html_content,
                        original_url=original_url,
                        crawled_at=crawled_at_from_minio,
                    )
                    # News 객체를 PostgreSQL에 INSERT
                    await pg_client.insert_news(news_item=news_item)

                    successfully_transformed_count += 1
                except Exception as e:
                    logger.error(
                        f"Error transforming or inserting news ID {news_id} to PostgreSQL: {e}"
                    )

        if i + batch_size < len(minio_objects_to_transform):
            logger.info(
                f"Waiting for {delay_between_batches} seconds until next DW batch..."
            )
            await asyncio.sleep(delay_between_batches)

    logger.info(
        f"Finished loading {successfully_transformed_count} news articles to Data Warehouse."
    )
