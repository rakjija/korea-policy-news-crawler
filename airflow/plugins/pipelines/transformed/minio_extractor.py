import asyncio
import logging

from clients.minio_client import MinioClient

logger = logging.getLogger(__name__)


async def extract_raws_from_minio(
    minio_endpoint: str,
    minio_bucket_name: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_objects_to_extract: list[dict],
    batch_size: int = 5,
    delay_between_batches: int = 1,
) -> list[tuple[str, dict, dict]]:  # (raw_html_content, metadata, original_object_info)
    """
    MinIO에 저장된 Raw HTML 파일을 다운로드합니다.
    """
    logger.info("Starting extraction of raw HTML from MinIO.")

    # MinIO 클라이언트 초기화
    minio_client = MinioClient(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    extracted_raw_data = []

    for i in range(0, len(minio_objects_to_extract), batch_size):
        batch_objects = minio_objects_to_extract[i : i + batch_size]
        logger.info(
            f"Processing extraction batch {i // batch_size + 1}: {len(batch_objects)} objects."
        )

        tasks = []
        for obj in batch_objects:
            object_name = obj["minio_path"]
            tasks.append(minio_client.download_file(minio_bucket_name, object_name))

        downloaded_results: list[
            tuple[bytes, dict] | BaseException
        ] = await asyncio.gather(*tasks, return_exceptions=True)

        for j, result in enumerate(downloaded_results):
            obj = batch_objects[j]  # original_object_info
            news_id = obj["news_id"]

            if isinstance(result, BaseException):
                logger.error(
                    f"Error downloading raw HTML for news ID {news_id} from MinIO: {result}"
                )
            else:
                raw_html_content, metadata = result

                extracted_raw_data.append(
                    (raw_html_content.decode("utf-8"), metadata, obj)
                )  # 원본 객체 정보도 함께 반환

        if i + batch_size < len(minio_objects_to_extract):
            logger.info(
                f"Waiting for {delay_between_batches} seconds until next extraction batch..."
            )
            await asyncio.sleep(delay_between_batches)

    logger.info(
        f"Finished extraction of {len(extracted_raw_data)} raw HTML files from MinIO."
    )
    return extracted_raw_data
