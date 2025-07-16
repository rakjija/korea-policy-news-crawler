import logging
import time

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def scrap_urls(
    url: str,
    start_date: str,  # 사용 가능한 가장 과거 일자: 1970-01-01
    end_date: str,
) -> list[str]:
    news_list = []
    page = 1
    while True:
        try:
            # form#mainForm
            form_data = {
                "pageIndex": page,
                "startDate": start_date,
                "endDate": end_date,
                "period": "direct",
            }
            res = httpx.post(url, data=form_data)
            res.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"{url} - {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

        soup = BeautifulSoup(res.content, "lxml")
        page_results = [
            f"https://www.korea.kr{a_tag['href']}"
            for a_tag in soup.select("div.article_wrap div.list_type li > a")
        ]

        if not page_results:
            logger.info("Finished scraping: No more news found")
            break

        news_list.extend(page_results)
        logger.info(f"Found {len(page_results)} news items on page {page}")

        page += 1
        time.sleep(1)

    return news_list
