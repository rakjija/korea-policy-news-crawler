import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pipelines.raw.minio_loader import load_raws_to_minio
from pipelines.raw.raw_scraper import scrap_raw_html_batch
from pipelines.raw.urls_scraper import scrap_urls_from_webpage
from pipelines.transformed.minio_extractor import extract_raws_from_minio
from pipelines.transformed.postgres_loader import load_transforms_to_postgres
from pipelines.transformed.raw_transformer import transform_raws

logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


async def main_async():
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

    try:
        # ==========================
        # ======= MAIN LOGIC =======
        # ==========================

        # 1. 뉴스 URL 리스트 크롤링 (Extract URLs)
        logger.info("Starting news list scraping...")
        urls = scrap_urls_from_webpage(
            url=f"{korea_kr_base_url}{korea_kr_list_path}",
            start_date=crawling_start_date,
            end_date=crawling_end_date,
        )
        logger.debug(f"Found {len(urls)} news URLs.")
        logger.info("Finished news list scraping.")

        # 2. Raw HTML 스크랩 (Extract Raw HTML)
        scraped_raw_data = []
        if not urls:
            logger.warning("No news URLs to process, skipping raw HTML scraping.")
        else:
            logger.info("Starting raw HTML scraping...")
            scraped_raw_data = await scrap_raw_html_batch(urls)
            logger.info(
                f"Finished raw HTML scraping of {len(scraped_raw_data)} articles."
            )

        # 3. Raw HTML을 MinIO에 저장 (Load Raw Data to Data Lake)
        minio_uploaded_objects = []
        if not scraped_raw_data:
            logger.info("Starting uploading raw HTML to MinIO...")
            minio_uploaded_objects = await load_raws_to_minio(
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                minio_bucket_name=minio_raw_news_bucket,
                scraped_raw_data=scraped_raw_data,
            )
            logger.info(
                f"Finished uploading {len(minio_uploaded_objects)} raw HTML files to MinIO."
            )
        else:
            logger.warning("No raw HTML data to upload to MinIO.")

        # 4. MinIO에서 데이터 추출 (Extract Raw Data from Data Lake)
        extracted_raw_data = []
        if minio_uploaded_objects:
            logger.info("Starting MinIO extraction...")
            extracted_raw_data = await extract_raws_from_minio(
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                minio_bucket_name=minio_raw_news_bucket,
                minio_objects_to_extract=minio_uploaded_objects,
            )
            logger.info(
                f"Finished MinIO extraction. Extracted {len(extracted_raw_data)} articles."
            )
        else:
            logger.warning("No MinIO objects to extract, skipping extraction.")

        # 5. 데이터 변환 (Transform Raw Data)
        transformed_data = []
        if extracted_raw_data:
            logger.info("Starting transforming raw data...")
            transformed_data = await transform_raws(
                raw_data=extracted_raw_data,
            )
            logger.info(
                f"Finished transforming raw data of {len(transformed_data)} news articles."
            )
        else:
            logger.warning("No data to transform, skipping transformation.")

        # 6. 변환된 데이터 PostgreSQL에 적재 (Load to Data Warehouse)
        if transformed_data:
            logger.info("Starting loading transformed data to PostgreSQL...")
            await load_transforms_to_postgres(
                news_items=transformed_data,
                pg_host=pg_host,
                pg_port=pg_port,
                pg_user=pg_user,
                pg_password=pg_password,
                pg_dbname=pg_dbname,
            )
            logger.info("Finished loading transformed data to PostgreSQL.")
        else:
            logger.warning("No transformed news items to load to PostgreSQL.")

    except Exception as e:
        logger.exception(f"An unexpected error occurred in the pipeline: {e}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
