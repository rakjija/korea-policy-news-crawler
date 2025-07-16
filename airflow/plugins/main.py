import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

from clients.minio_client import MinioClient
from dotenv import load_dotenv

# from clients.postgres_client import PostgresClient # ⬅️ 더 이상 직접 사용하지 않으므로 제거
from pipelines.raw.dl_loader import load_to_dl
from pipelines.raw.list_scraper import scrap_urls
from pipelines.raw.raw_scraper import scrap_raw_html_batch
from pipelines.transformed.dl_extractor import (  # ⬅️ 임포트 변경
    extract_raws_from_minio,
    transform_downloaded_html,
)
from pipelines.transformed.dw_loader import load_to_dw

logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


async def main_async():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # .env 설정값 가져오기
    korea_kr_base_url = os.getenv("KOREA_KR_BASE_URL")
    korea_kr_list_path = os.getenv("KOREA_KR_LIST_PATH")
    crawling_start_date = os.getenv(
        "CRAWLING_START_DATE",
        datetime.today().strftime("%Y-%m-%d"),
    )
    crawling_end_date = os.getenv(
        "CRAWLING_END_DATE",
        datetime.today().strftime("%Y-%m-%d"),
    )
    # __Minio
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_raw_news_bucket = os.getenv("MINIO_RAW_NEWS_BUCKET", "raw-news")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "myuser")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "mypassword")
    # __Postgres
    pg_host = os.getenv("POSTGRES_HOST", "postgresql")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_user = os.getenv("POSTGRES_USER", "myuser")
    pg_password = os.getenv("POSTGRES_PASSWORD", "mypassword")
    pg_dbname = os.getenv("POSTGRES_DBNAME", "mydatabase")

    # 1. 뉴스 리스트 추출 (Extract URLs)
    logger.info("Starting news list scraping...")
    urls = scrap_urls(
        url=f"{korea_kr_base_url}{korea_kr_list_path}",
        start_date=crawling_start_date,
        end_date=crawling_end_date,
    )
    logger.info(f"Found {len(urls)} news URLs.")

    # 2. Raw HTML 스크랩 (Extract Raw HTML)
    scraped_raw_data = []
    if not urls:
        logger.warning("No news URLs to process, skipping raw HTML scraping.")
    else:
        logger.info("Starting raw HTML scraping...")
        scraped_raw_data = await scrap_raw_html_batch(urls)
        logger.info(f"Finished raw HTML scraping of {len(scraped_raw_data)} articles.")

    # 3. Raw HTML을 MinIO에 저장 (Load to Data Lake)
    minio_uploaded_objects = []
    if not scraped_raw_data:
        logger.warning("No raw HTML data to upload to MinIO.")
    else:
        logger.info("Starting uploading raw HTML to MinIO...")
        minio_uploaded_objects = await load_to_dl(
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket_name=minio_raw_news_bucket,
            scraped_raw_data=scraped_raw_data,
        )
        logger.info(
            f"Finished uploading {len(minio_uploaded_objects)} raw HTML files to MinIO."
        )

    # 4. MinIO에서 데이터 추출 및 변환 (Extract & Transform)
    transformed_news_items = []
    if minio_uploaded_objects:
        logger.info("Starting MinIO extraction and transformation...")
        minio_client = MinioClient(  # MinioClient는 이미 위에서 초기화되었지만, 명확성을 위해 다시 초기화
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        # ⬅️ extract_raw_html_from_minio 직접 호출
        downloaded_raw_data = await extract_raws_from_minio(
            minio_client=minio_client,
            minio_bucket_name=minio_raw_news_bucket,
            minio_objects_to_extract=minio_uploaded_objects,
        )
        # ⬅️ transform_downloaded_html 직접 호출
        transformed_news_items = await transform_downloaded_html(
            downloaded_raw_data=downloaded_raw_data,
        )
        logger.info(
            f"Finished MinIO extraction and transformation of {len(transformed_news_items)} news articles."
        )
    else:
        logger.warning("No MinIO objects to transform, skipping transformation.")

    # 5. 변환된 데이터 PostgreSQL에 적재 (Load to Data Warehouse)
    if not transformed_news_items:
        logger.warning("No transformed news items to load to PostgreSQL.")
    else:
        logger.info("Starting loading transformed data to PostgreSQL...")
        await load_to_dw(
            news_items=transformed_news_items,
            pg_host=pg_host,
            pg_port=pg_port,
            pg_user=pg_user,
            pg_password=pg_password,
            pg_dbname=pg_dbname,
        )
        logger.info("Finished loading transformed data to PostgreSQL.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
