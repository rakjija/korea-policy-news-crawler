import asyncio
import logging

import httpx

from crawler import news_list
from crawler.news import scrap_news
from utils.get_headers import get_headers

logger = logging.getLogger(__name__)


async def process_news_articles_in_batches(
    news_urls: list[str],
    batch_size: int = 5,
    delay_between_batches: int = 2,
):
    """
    뉴스 URL 리스트를 받아 배치 단위로 비동기 스크래핑 처리
    각 배치는 지정된 시간만큼 지연을 둠
    """

    logger.info(
        f"Starting batch processing of {len(news_urls)} news articles, {batch_size} per batch."
    )
    processed_articles = []

    async with httpx.AsyncClient(
        headers=get_headers("https://www.korea.kr"),
    ) as client:
        for i in range(0, len(news_urls), batch_size):
            batch_urls = news_urls[i : i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1}: {len(batch_urls)} articles."
            )

            tasks = [scrap_news(url, client) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error processing news article: {result}")
                else:
                    processed_articles.append(result)

            if i + batch_size < len(news_urls):
                logger.info(
                    f"Waiting for {delay_between_batches} seconds until next batch..."
                )
                await asyncio.sleep(delay_between_batches)

    logger.info(f"Finished processing {len(processed_articles)} news articles.")
    return processed_articles


async def main_async():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logger.info("Starting news list scraping...")
    news_urls = news_list.scrap_news_list("https://www.korea.kr/news/policyNewsList.do")
    logger.info(f"Found {len(news_urls)} news URLs.")

    if news_urls:
        await process_news_articles_in_batches(news_urls)
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
