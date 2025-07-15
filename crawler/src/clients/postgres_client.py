import json
import logging

import asyncpg

from models.news import News

logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        dbname,
    ):
        self.conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(self.conn_string)
            logger.info("Successfully connected to PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed.")

    async def create_news_table(self):
        """
        테이블이 없을 경우 생성
        """
        if not self.pool:
            logger.error("Connection pool is not initialized.")
            raise

        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    id BIGINT PRIMARY KEY,
                    title TEXT NOT NULL,
                    subtitles TEXT[],
                    publisher TEXT,
                    contents TEXT NOT NULL,
                    images JSONB[],
                    url TEXT NOT NULL,
                    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    crawled_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
            """)
            logger.info("News table created or already exists.")

    async def insert_news(self, news_item: News):
        if not self.pool:
            logger.error("Connection pool is not initialized.")
            raise

        async with self.pool.acquire() as conn:
            images_jsonb = [json.dumps(img.model_dump()) for img in news_item.images]
            try:
                await conn.execute(
                    """
                    INSERT INTO news (
                        id, title, subtitles, publisher, contents, images, url, published_at, crawled_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        subtitles = EXCLUDED.subtitles,
                        publisher = EXCLUDED.publisher,
                        contents = EXCLUDED.contents,
                        images = EXCLUDED.images,
                        url = EXCLUDED.url,
                        published_at = EXCLUDED.published_at,
                        crawled_at = EXCLUDED.crawled_at;
                """,
                    news_item.id,
                    news_item.title,
                    news_item.subtitles,
                    news_item.publisher,
                    news_item.contents,
                    images_jsonb,  # JSONB[] 타입으로 전달
                    news_item.url,
                    news_item.published_at,
                    news_item.crawled_at,
                )
                logger.info(
                    f"News ID {news_item.id} inserted/updated successfully in PostgreSQL."
                )
            except Exception as e:
                logger.error(
                    f"Failed to insert/update news ID {news_item.id} into PostgreSQL: {e}"
                )
                raise
