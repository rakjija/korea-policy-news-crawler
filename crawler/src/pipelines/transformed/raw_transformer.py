import json
import logging
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from models.news import Image, News

logger = logging.getLogger(__name__)


def transform_raw_html(
    raw_html_content: bytes,
    original_url: str,
    crawled_at: datetime,
):
    """
    Raw HTML을 파싱하여 News 객체로 변환
    """
    soup = BeautifulSoup(raw_html_content, "lxml")

    # news id
    if news_id_qs := parse_qs(urlparse(original_url).query).get("newsId"):
        news_id = int(news_id_qs[0])
        logger.info(f"News ID: {news_id} - Successfully extracted.")
    else:
        logger.error("News ID not found in URL.")
        raise

    # title
    if title_tag := soup.select_one("div.view_title > h1"):
        title = title_tag.get_text().strip()
        logger.info(f"Title: '{title}' - Successfully extracted.")
    else:
        logger.error("Title not found.")
        raise

    # subtitles
    subtitles = []
    if subtitles_tag := soup.select_one("div.article_head > h2"):
        subtitles = [
            el.get_text() for el in subtitles_tag.contents if el.get_text() != ""
        ]
        logger.info(f"Subtitles: {subtitles} - Successfully extracted.")
    else:
        logger.warning("No subtitles found.")

    # publisher
    publisher = ""
    if publisher_tag := soup.select("div.info span")[1]:
        for i in publisher_tag.select("i"):
            i.decompose()
        publisher = publisher_tag.get_text().strip()
        logger.info(f"Publisher: '{publisher}' - Successfully extracted.")
    else:
        logger.warning("No publisher found.")

    # contents
    if contents_tag := soup.select_one("div.view_cont"):
        contents: str = contents_tag.get_text().replace("\xa0", "").strip()
        logger.info(
            f"Contents: '{contents[:15]}...{contents[-15:]}' - Successfully extracted."
        )
    else:
        logger.error("Contents not found.")
        raise

    # images
    images: list[Image] = []
    if image_tags := soup.select("span.imageSpan > img"):
        for tag in image_tags:
            image_url = str(tag["src"])
            image_comments = str(tag["alt"])
            image = Image(url=image_url, comments=image_comments)
            images.append(image)
        logger.info(f"Images: {images} - Successfully extracted.")
    else:
        logger.warning("No images found.")

    # __get json data
    json_data = {}
    if json_tag := soup.select_one('script[type="application/ld+json"]'):
        try:
            json_data = json.loads(json_tag.get_text())
            logger.info(
                f"JSON data: {json.dumps(json_data, indent=2, ensure_ascii=False)} - Successfully extracted."
            )
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {e}")
            raise ValueError(f"Error decoding JSON data: {e}")
    else:
        logger.warning("No JSON data found.")

    # tags
    tags = []
    if "keyword" in json_data and json_data["keyword"]:
        tags = json_data["keyword"].split(",")
        logger.info(f"Tags: {tags} - Successfully extracted.")
    else:
        logger.warning("No tags found in JSON data.")

    # published_at
    published_at = None
    if "datePublished" in json_data and json_data["datePublished"]:
        try:
            published_at = datetime.fromisoformat(json_data["datePublished"])
            logger.info(f"Published at: {published_at} - Successfully extracted.")
        except ValueError as e:
            logger.error(f"Error parsing published_at date: {e}")
            raise ValueError(f"Error parsing published_at date: {e}")
    else:
        logger.error("Published date not found in JSON data.")
        raise

    news = News(
        id=news_id,
        title=title,
        subtitles=subtitles,
        publisher=publisher,
        contents=contents,
        images=images,
        url=original_url,
        published_at=published_at,
        crawled_at=crawled_at,
    )

    return news
