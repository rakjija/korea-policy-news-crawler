import asyncio
import logging

from clients.postgres_client import PostgresClient
from models.news import News

logger = logging.getLogger(__name__)


async def load_transforms_to_postgres(
    news_items: list[News],
    pg_host: str,
    pg_port: str,
    pg_user: str,
    pg_password: str,
    pg_dbname: str,
    batch_size: int = 5,
    delay_between_batches: int = 1,
):
    """
    변환된 News 객체를 PostgreSQL(DW)에 저장합니다.
    """
    logger.info("Starting loading transformed data to Data Warehouse.")

    pg_client = PostgresClient(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname,
    )
    await pg_client.connect()
    await pg_client.create_news_table()

    successfully_loaded_count = 0

    for i in range(0, len(news_items), batch_size):
        batch_news_items = news_items[i : i + batch_size]
        logger.info(
            f"Processing DW loading batch {i // batch_size + 1}: {len(batch_news_items)} articles."
        )

        tasks = []
        for news_item in batch_news_items:
            tasks.append(pg_client.insert_news(news_item=news_item))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for j, result in enumerate(results):
            news_item = batch_news_items[j]
            if isinstance(result, Exception):
                logger.error(
                    f"Error inserting news ID {news_item.id} into PostgreSQL: {result}"
                )
            else:
                successfully_loaded_count += 1

        if i + batch_size < len(news_items):
            logger.info(
                f"Waiting for {delay_between_batches} seconds until next DW loading batch..."
            )
            await asyncio.sleep(delay_between_batches)

    await pg_client.close()
    logger.info(
        f"Finished loading {successfully_loaded_count} news articles to Data Warehouse."
    )
