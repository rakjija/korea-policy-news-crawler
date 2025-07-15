import json
import logging
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup

from models.news import Image, News

logger = logging.getLogger(__name__)


async def scrap_news(url: str, client: httpx.AsyncClient) -> News:
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

    soup = BeautifulSoup(res.content, "lxml")

    # news id
    if news_id_qs := parse_qs(urlparse(url).query).get("newsId"):
        news_id = int(news_id_qs[0])
        logger.info(f"{news_id} - Get news_id successfully")
    else:
        logger.error("No news_id")
        raise

    # title
    if title_tag := soup.select_one("div.view_title > h1"):
        title = title_tag.get_text().strip()
        logger.info(f"{title} - Get title successfully")
    else:
        logger.error("No title")
        raise

    # subtitles
    if subtitles_tag := soup.select_one("div.article_head > h2"):
        subtitles = [
            el.get_text() for el in subtitles_tag.contents if el.get_text() != ""
        ]

        logger.info(f"{subtitles} - Get subtitles successfully")
    else:
        logger.error("No subtitles")
        raise

    # publisher
    if publisher_tag := soup.select("div.info span")[1]:
        for i in publisher_tag.select("i"):
            i.decompose()
        publisher = publisher_tag.get_text().strip()
        logger.info(f"{publisher} - Get publisher successfully")
    else:
        logger.error("No publisher")
        raise

    # contents
    if contents_tag := soup.select_one("div.view_cont"):
        contents: str = contents_tag.get_text().replace("\xa0", "").strip()
        logger.info(f"{contents[:15]}...{contents[-15:]} - Get contents successfully")
    else:
        logger.error("No contents")
        raise

    # images
    images: list[Image] = []
    if image_tags := soup.select("span.imageSpan > img"):
        for tag in image_tags:
            image_url = str(tag["src"])
            image_comments = str(tag["alt"])
            image = Image(url=image_url, comments=image_comments)
            images.append(image)
        logger.info(f"{images} - Get images successfully")
    else:
        logger.warning("No images")

    # get json data
    if json_tag := soup.select_one('script[type="application/ld+json"]'):
        json_data = json.loads(json_tag.get_text())
        logger.info(f"{json_data} - Get json_data successfully")
    else:
        logger.warning("No json_data")
        raise

    # tags
    tags = json_data["keyword"].split(",")
    logger.info(f"{tags} - Get tags successfully")

    # published_at
    published_at = datetime.fromisoformat(json_data["datePublished"])
    logger.info(f"Published at: {published_at} - Successfully extracted.")

    # crawled_at
    crawled_at = datetime.now()
    logger.info(f"Crawled at: {crawled_at} - Successfully extracted.")

    news = News(
        id=news_id,
        title=title,
        subtitles=subtitles,
        publisher=publisher,
        contents=contents,
        images=images,
        url=url,
        published_at=published_at,
        crawled_at=crawled_at,
    )

    return news
