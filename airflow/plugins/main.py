import asyncio
import logging

from clients.minio_client import MinioClient
from clients.postgres_client import PostgresClient
from pipelines.raw.dl_loader import load_to_dl
from pipelines.raw.list_scraper import scrap_news_list
from pipelines.transformed.dw_loader import load_to_dw

logger = logging.getLogger(__name__)


async def main_async():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # 뉴스 리스트 추출
    logger.info("Starting news list scraping...")
    news_urls = scrap_news_list("https://www.korea.kr/news/policyNewsList.do")
    logger.info(f"Found {len(news_urls)} news URLs.")

    # MinIO 저장
    if news_urls:
        # TODO: 환경 변수나 설정 파일에서 DB 정보 로그
        pg_client = PostgresClient(
            host="localhost",
            port="5432",
            user="myuser",
            password="mypassword",
            dbname="mydatabase",
        )
        await pg_client.connect()
        await pg_client.create_news_table()

        minio_uploaded_objects = await load_to_dl(news_urls)

        if minio_uploaded_objects:
            minio_client = MinioClient(
                endpoint="localhost:9000",
                access_key="ROOTNAME",
                secret_key="CHANGEME123",
                secure=False,
            )

            await load_to_dw(
                minio_client=minio_client,
                pg_client=pg_client,
                minio_bucket_name="raw-news",
                minio_objects_to_transform=minio_uploaded_objects,
            )
        else:
            logger.warning("No objects uploaded to MinIO for DW processing.")

        await pg_client.close()
    else:
        logger.warning("No news URLs to process.")


def main():
    asyncio.run(main_async())

    # __TEST
    # result = asyncio.run(
    #     scrap_news(
    #         "https://www.korea.kr/news/reporterView.do?newsId=148942535&pWise=sub&pWiseSub=I1"
    #     )
    # )
    # print(result)


if __name__ == "__main__":
    main()
