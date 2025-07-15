from datetime import datetime

from pydantic import BaseModel


class Image(BaseModel):
    url: str
    comments: str


class News(BaseModel):
    id: int
    title: str
    subtitles: list[str]
    publisher: str
    contents: str
    images: list[Image]
    url: str
    published_at: datetime
    crawled_at: datetime
