import asyncio
import logging

from pipelines.raw.dl_loader import load_to_dl
from pipelines.raw.list_scraper import scrap_news_list

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
        minio_uploaded_objects = await load_to_dl(news_urls)
    else:
        logger.warning("No news URLs to process.")


def main():
    asyncio.run(main_async())
    # result = asyncio.run(
    #     scrap_news(
    #         "https://www.korea.kr/news/reporterView.do?newsId=148942535&pWise=sub&pWiseSub=I1"
    #     )
    # )
    # print(result)


if __name__ == "__main__":
    main()
