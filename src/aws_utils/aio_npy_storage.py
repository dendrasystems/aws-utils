import logging
import aioboto3
import aiofiles
import orjson
import numpy as np
from botocore.exceptions import ClientError
from pathlib import Path
from typing import Any, AsyncGenerator

from .npy_storage import METADATA_KEY, _normalize_url


logger = logging.getLogger(__name__)


class AIONpyReader:
    """
    For asyncio-based reading of numpy arrays from S3 with optional local caching.
    Uses memory efficient chunk-based reading using HTTP range requests.
    """

    def __init__(
        self, bucket: str, key_prefix: str, cache_dir: str | None = None, client=None
    ):
        self.bucket = bucket
        self.key_prefix = key_prefix.rstrip("/")
        self._metadata_cache: dict[str, Any] | None = None
        self._shard_metadata: list[dict[str, Any]] = []
        self._cache_locally = cache_dir is not None
        self._local_cache_dir = Path(cache_dir) if cache_dir else None
        self._metadata_key = f"{self.key_prefix}/{METADATA_KEY}"
        self._client = client
        self._session = aioboto3.Session()

    def _get_shard_key(self, shard_filename: str) -> str:
        return f"{self.key_prefix}/{shard_filename}"

    async def _get_client(self):
        """Get the S3 client, creating one if not provided."""
        if self._client is not None:
            return self._client
        return self._session.client("s3")

    async def prewarm_cache(self) -> None:
        """
        Preload the local cache with the metadata and shards.
        """
        if not self._cache_locally:
            raise ValueError("Cannot prewarm cache without local cache directory")
        metadata_path = self._local_cache_dir / self._metadata_key
        if not metadata_path.exists():
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Preloading metadata to local cache")
            async with await self._get_client() as client:
                await client.download_file(
                    Bucket=self.bucket,
                    Key=self._metadata_key,
                    Filename=str(metadata_path),
                )

        async with aiofiles.open(metadata_path) as f:
            content = await f.read()
            self._metadata_cache = orjson.loads(content)

        for shard in self._metadata_cache["shards"]:
            shard_key = self._get_shard_key(shard["filename"])
            shard_path = self._local_cache_dir / shard_key
            if not shard_path.exists():
                shard_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Preloading shard {shard['filename']} to local cache")
                async with await self._get_client() as client:
                    await client.download_file(
                        Bucket=self.bucket, Key=shard_key, Filename=str(shard_path)
                    )

    async def check_exists(self) -> bool:
        """
        Check if the embeddings exist in S3 by checking for metadata file.
        """
        logger.info(
            f"Checking if embeddings exist in S3 at s3://{self.bucket}/{self._metadata_key}"
        )
        if (
            self._cache_locally
            and (self._local_cache_dir / self._metadata_key).exists()
        ):
            return True

        try:
            async with await self._get_client() as client:
                await client.head_object(Bucket=self.bucket, Key=self._metadata_key)
            return True
        except ClientError:
            return False

    async def _get_metadata(self) -> dict[str, Any]:
        """Get metadata from S3 with caching."""
        if self._metadata_cache is None:
            if self._cache_locally:
                async with aiofiles.open(
                    self._local_cache_dir / self._metadata_key
                ) as f:
                    content = await f.read()
                    self._metadata_cache = orjson.loads(content)
            else:
                async with await self._get_client() as client:
                    response = await client.get_object(
                        Bucket=self.bucket, Key=self._metadata_key
                    )
                    self._metadata_cache = orjson.loads((await response["Body"].read()))
        return self._metadata_cache

    async def _range_read(
        self, embeddings_key: str, start_byte: int, end_byte: int
    ) -> bytes:
        """
        Read a range of bytes from S3.
        """
        if self._cache_locally:
            async with aiofiles.open(self._local_cache_dir / embeddings_key, "rb") as f:
                await f.seek(start_byte)
                return await f.read(end_byte - start_byte + 1)
        else:
            async with await self._get_client() as client:
                response = await client.get_object(
                    Bucket=self.bucket,
                    Key=embeddings_key,
                    Range=f"bytes={start_byte}-{end_byte}",
                )
                return await response["Body"].read()

    async def _read_shard_chunk(
        self, shard: dict[str, Any], start_idx: int, end_idx: int
    ) -> np.ndarray:
        """
        Read a range of rows from S3.
        """
        embeddings_key = self._get_shard_key(shard["filename"])
        data_offset = shard["data_offset"]
        dtype = np.dtype(shard["dtype"])
        embedding_dim = shard["embedding_dim"]
        bytes_per_row = embedding_dim * dtype.itemsize
        chunk_rows = end_idx - start_idx

        # Calculate byte range for this chunk
        start_byte = data_offset + (start_idx * bytes_per_row)
        end_byte = start_byte + (chunk_rows * bytes_per_row) - 1

        chunk_bytes = await self._range_read(embeddings_key, start_byte, end_byte)

        # Convert bytes back to numpy array
        return np.frombuffer(chunk_bytes, dtype=dtype).reshape(
            chunk_rows, embedding_dim
        )

    async def get_sample_count(self) -> int:
        """Get total number of samples across all shards without loading data."""
        metadata = await self._get_metadata()
        total_samples: int = metadata["total_samples"]
        return total_samples

    async def read_embeddings_chunked(
        self, chunk_size: int = 1000
    ) -> AsyncGenerator[tuple[np.ndarray, list[str]], None]:
        """
        Stream read embeddings in chunks from all shards using range requests.
        Transparently handles multiple shard files.

        Args:
            chunk_size: Number of samples to read per chunk

        Yields:
            Tuple of (embeddings_chunk, urls_chunk)
        """
        metadata = await self._get_metadata()
        shards = metadata["shards"]

        for shard in shards:
            n_samples = shard["n_samples"]
            urls = shard["urls"]

            # Process this shard in chunks
            for start_idx in range(0, n_samples, chunk_size):
                end_idx = min(start_idx + chunk_size, n_samples)
                embeddings_chunk = await self._read_shard_chunk(
                    shard, start_idx, end_idx
                )
                urls_chunk = urls[start_idx:end_idx]
                yield embeddings_chunk, urls_chunk

    async def read_embeddings_by_urls(
        self, urls: list[str]
    ) -> tuple[np.ndarray, list[str]]:
        """
        Read embeddings for specific URLs using targeted byte range requests.

        Args:
            urls: List of S3 URLs to retrieve embeddings for

        Returns:
            Tuple of (embeddings, found_urls) where embeddings are in the same order as input URLs.
            Only URLs that exist in the storage will be returned.
        """
        metadata = await self._get_metadata()
        shards = metadata["shards"]
        embedding_dim = metadata["embedding_dim"]

        # Build lookup with normalized keys so that full s3://bucket/key URLs and
        # bare key paths both resolve to the same entry regardless of how the NPY
        # metadata was written.
        url_to_location = {}
        for shard_idx, shard in enumerate(shards):
            for url_idx, url in enumerate(shard["urls"]):
                url_to_location[_normalize_url(url)] = (shard_idx, url_idx)

        # Prepare results in the same order as input
        embeddings_list = []
        found_urls = []

        for url in urls:
            if _normalize_url(url) in url_to_location:
                shard_idx, url_idx = url_to_location[_normalize_url(url)]
                shard = shards[shard_idx]
                embedding = await self._read_shard_chunk(shard, url_idx, url_idx + 1)
                embeddings_list.append(embedding)
                found_urls.append(url)

        if not embeddings_list:
            return np.array([]).reshape(0, embedding_dim), []

        # Combine all embeddings
        combined_embeddings = np.vstack(embeddings_list)
        return combined_embeddings, found_urls
