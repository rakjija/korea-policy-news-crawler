import asyncio
import logging
from urllib.parse import parse_qs, urlparse

import httpx

from utils.headers_generator import get_headers

logger = logging.getLogger(__name__)


async def scrap_raw_html(url: str, client: httpx.AsyncClient):
    """
    URL에서 뉴스 페이지의 Raw HTML와 뉴스 ID를 스크랩합니다.
    반환값: tuple(raw_html_content, original_url, news_id)
    """
    try:
        res = await client.get(url)
        res.raise_for_status()
        logger.info(f"{url} - {res.status_code}")
    except httpx.HTTPStatusError as e:
        logger.error(f"{url} - {e.response.status_code}")
        raise
    except httpx.RequestError as e:
        logger.error(f"Request failed for {url}: {e}")
        raise

    news_id = None
    if news_id_qs := parse_qs(urlparse(url).query).get("newsId"):
        try:
            news_id = int(news_id_qs[0])
            logger.info(f"News ID: {news_id} - Successfully extracted")
        except ValueError:
            logger.error(f"Could not convert newsId '{news_id_qs[0]}' to integer.")
            raise
    else:
        logger.error("News ID not found in URL.")
        raise

    return res.content, url, news_id


async def scrap_raw_html_batch(
    news_urls: list[str],
    batch_size: int = 5,
    delay_between_batches: int = 2,
):
    """
    뉴스 URL 리스트를 받아 배치 단위로 비동기 스크래핑 처리합니다.
    각 배치는 지정된 시간만큼 지연을 둡니다.
    """

    logger.info(
        f"Starting batch processing of {len(news_urls)} news articles, {batch_size} per batch."
    )
    processed_articles = []

    async with httpx.AsyncClient(
        headers=get_headers(referer="https://www.korea.kr"),
    ) as client:
        for i in range(0, len(news_urls), batch_size):
            batch_urls = news_urls[i : i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1}: {len(batch_urls)} articles."
            )

            tasks = [scrap_raw_html(url, client) for url in batch_urls]
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
