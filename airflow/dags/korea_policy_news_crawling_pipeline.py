from __future__ import annotations

import asyncio
import os
from datetime import datetime

import pendulum

# plugins 폴더에서 파이프라인의 각 단계를 구성하는 함수들을 직접 import 합니다.
from pipelines.raw.minio_loader import load_raws_to_minio
from pipelines.raw.raw_scraper import scrap_raw_html_batch
from pipelines.raw.urls_scraper import scrap_urls_from_webpage
from pipelines.transformed.minio_extractor import extract_raws_from_minio
from pipelines.transformed.postgres_loader import load_transforms_to_postgres
from pipelines.transformed.raw_transformer import transform_raws

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param


@dag(
    dag_id="korea_policy_news_crawling_pipeline",
    start_date=pendulum.datetime(2024, 7, 16, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    # Airflow UI에서 'Trigger DAG w/ config'를 통해 동적으로 날짜를 지정
    params={
        "start_date": Param(
            datetime.today().strftime("%Y-%m-%d"),
            type="string",
            title="Crawling Start Date",
        ),
        "end_date": Param(
            datetime.today().strftime("%Y-%m-%d"),
            type="string",
            title="Crawling End Date",
        ),
    },
    doc_md="""
    대한민국 정책 브리핑(https://www.korea.kr/)의 뉴스 기사를 
    크롤링하고, 처리하여 최종적으로 데이터 웨어하우스에 적자해는 파이프라인입니다.
    주요 로직은 plugins 디렉토리에서 가져옵니다.
    """,
)
def korea_policy_news_crawling_dag():
    @task
    def get_configs(**context):
        """
        Airflow 파라미터와 환경 변수에서 설정을 가져옵니다.
        (실제 운영 환경에서는 Airflow Variable과 Connection 사용을 권장합니다.)
        """
        return {
            "korea_kr_base_url": os.getenv("KOREA_KR_BASE_URL"),
            "korea_kr_list_path": os.getenv("KOREA_KR_LIST_PATH"),
            "crawling_start_date": context["params"]["start_date"],
            "crawling_end_date": context["params"]["end_date"],
            "minio_endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "minio_raw_news_bucket": os.getenv("MINIO_RAW_NEWS_BUCKET", "raw-news"),
            "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "myuser"),
            "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "mypassword"),
            "pg_host": os.getenv("POSTGRES_HOST", "postgresql"),
            "pg_port": os.getenv("POSTGRES_PORT", "5432"),
            "pg_user": os.getenv("POSTGRES_USER", "myuser"),
            "pg_password": os.getenv("POSTGRES_PASSWORD", "mypassword"),
            "pg_dbname": os.getenv("POSTGRES_DBNAME", "mydatabase"),
        }

    @task_group(group_id="data_lake_pipeline")  # type: ignore[arg-type]
    def data_lake_pipeline_group(configs: dict):
        """
        뉴스 URL을 추출하고, Raw HTML을 스크랩하여 MinIO (Data Lake)에 적재하는 그룹입니다.
        """

        @task
        def extract_news_urls(configs: dict) -> list[str]:
            """웹페이지에서 뉴스 기사 URL 목록을 추출합니다."""
            urls = scrap_urls_from_webpage(
                url=f"{configs['korea_kr_base_url']}{configs['korea_kr_list_path']}",
                start_date=configs["crawling_start_date"],
                end_date=configs["crawling_end_date"],
            )
            return urls

        @task
        def scrap_raw_html(news_urls: list[str]) -> list:
            """URL 목록을 받아 Raw HTML을 스크랩합니다."""
            if not news_urls:
                return []
            return asyncio.run(scrap_raw_html_batch(news_urls))

        @task
        def load_raw_to_minio(scraped_data: list, configs: dict) -> list:
            """스크랩한 Raw HTML를 MinIO에 업로드합니다."""
            if not scraped_data:
                return []
            uploaded_objects = asyncio.run(
                load_raws_to_minio(
                    minio_endpoint=configs["minio_endpoint"],
                    minio_access_key=configs["minio_access_key"],
                    minio_secret_key=configs["minio_secret_key"],
                    minio_bucket_name=configs["minio_raw_news_bucket"],
                    scraped_raw_data=scraped_data,
                )
            )
            return uploaded_objects

        urls = extract_news_urls(configs)
        scraped_data = scrap_raw_html(urls)  # type: ignore[arg-type]
        minio_objects = load_raw_to_minio(scraped_data, configs)  # type: ignore[arg-type]
        return minio_objects  # Return the output for the next stage

    @task_group(group_id="data_warehouse_pipeline")
    def data_warehouse_pipeline_group(minio_objects: list, configs: dict):
        """MinIO에서 데이터를 추출, 변환하고 PostgreSQL (Data Warehouse)에 적재하는 그룹입니다."""

        @task
        def extract_raw_from_minio(objects_to_extract: list, configs: dict) -> list:
            """MinIO에서 원시 데이터를 추출합니다."""
            if not objects_to_extract:
                return []
            return asyncio.run(
                extract_raws_from_minio(
                    minio_endpoint=configs["minio_endpoint"],
                    minio_access_key=configs["minio_access_key"],
                    minio_secret_key=configs["minio_secret_key"],
                    minio_bucket_name=configs["minio_raw_news_bucket"],
                    minio_objects_to_extract=objects_to_extract,
                )
            )

        @task
        def transform_raw_data(raw_data: list) -> list:
            """원시 데이터를 구조화된 데이터로 변환합니다."""
            if not raw_data:
                return []
            return asyncio.run(transform_raws(raw_data))

        @task
        def load_to_postgres(transformed_data: list, configs: dict):
            """변환된 데이터를 PostgreSQL에 적재합니다."""
            if not transformed_data:
                print("No transformed data to load.")
                return
            asyncio.run(
                load_transforms_to_postgres(
                    news_items=transformed_data,
                    pg_host=configs["pg_host"],
                    pg_port=configs["pg_port"],
                    pg_user=configs["pg_user"],
                    pg_password=configs["pg_password"],
                    pg_dbname=configs["pg_dbname"],
                )
            )

        # Task Group 내의 데이터 흐름을 정의합니다.
        raw_data = extract_raw_from_minio(minio_objects, configs)
        transformed_data = transform_raw_data(raw_data)  # type: ignore[arg-type]
        load_to_postgres(transformed_data, configs)  # type: ignore[arg-type]

    # === DAG의 전체 워크플로우를 정의합니다 ===
    configs = get_configs()
    minio_objects_from_lake = data_lake_pipeline_group(configs)  # type: ignore[arg-type]

    # load_raw_to_minio 태스크가 성공적으로 끝나면 transform_and_load_group을 실행합니다.
    data_warehouse_pipeline_group(minio_objects_from_lake, configs)  # type: ignore[arg-type]


# DAG 파일을 Airflow가 인식할 수 있도록 마지막에 함수를 호출합니다.
korea_policy_news_crawling_dag()
