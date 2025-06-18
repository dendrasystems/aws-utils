"""
An implementation of a npy wrapper format for storing numpy arrays in S3.

We wrap the standard npy format with a metadata file that enables efficient chunk-based reading using HTTP range requests
as well as looking up specific embeddings by URL.

The metadata file is stored in the same bucket as the numpy arrays, and is named "embeddings_metadata.json".

The metadata file is a JSON file that contains the following fields:

- shards: a list of dictionaries, each containing the following fields:
    - shard_id: the id of the shard
    - filename: the name of the shard file
    - n_samples: the number of samples in the shard
    - embedding_dim: the dimension of the embeddings in the shard
    - dtype: the dtype of the embeddings in the shard
    - shape: the shape of the embeddings in the shard
    - data_offset: the offset of the data in the shard file
    - urls: the URLs of the embeddings in the shard
- total_samples: the total number of samples across all shards
- embedding_dim: the dimension of the embeddings across all shards
- n_shards: the number of shards
"""

import io
import json
import logging
import struct
from collections.abc import Generator
from pathlib import Path
from typing import Any

import numpy as np
from numpy.lib.format import MAGIC_LEN, read_magic

from .s3 import _get_client


logger = logging.getLogger(__name__)


METADATA_KEY = "embeddings_metadata.json"


class NpyWriter:
    """
    S3-compatible numpy array writer.
    """

    def __init__(self, bucket: str, key_prefix: str, client=None):
        self.bucket = bucket
        self.key_prefix = key_prefix.rstrip("/")
        self._shard_metadata: list[dict[str, Any]] = []
        self._metadata_key = f"{self.key_prefix}/{METADATA_KEY}"
        self._client = client or _get_client()

    def _get_numpy_data_offset(self, npy_data: bytes) -> int:
        """
        Determine the offset of the numpy data in the file. This is not available
        via the numpy library, so we need to parse the header manually.

        Format is MAGIC_LEN + HEADER_LEN_VALUE + HEADER_LEN.

        For version 1, the next 2 bytes form a little-endian unsigned short int: the length of the header data HEADER_LEN.
        For version 2 and 3, the next 4 bytes form a little-endian unsigned int: the length of the header data HEADER_LEN.
        """
        fp = io.BytesIO(npy_data)
        major, _ = read_magic(fp)
        if major > 3:
            raise ValueError("Unsupported numpy version")
        header_len_type = (
            "<H" if major == 1 else "<I"
        )  # these are not publically exposed by numpy
        header_len_str = fp.read(struct.calcsize(header_len_type))
        header_len = struct.unpack(header_len_type, header_len_str)[0]
        data_offset: int = MAGIC_LEN + struct.calcsize(header_len_type) + header_len
        return data_offset

    def write_embeddings(
        self, embeddings: np.ndarray, urls: list[str], shard_id: int
    ) -> None:
        """
        Write embeddings and URLs for a single shard to S3 as numpy arrays.

        Args:
            embeddings: Array of shape (n_samples, embedding_dim)
            urls: List of S3 URLs corresponding to embeddings
            shard_id: Identifier for this shard
        """
        embeddings_filename = f"embeddings_shard_{shard_id}.npy"
        embeddings_key = f"{self.key_prefix}/{embeddings_filename}"

        # Convert embeddings to bytes
        embeddings_float16 = embeddings.astype(np.float16)
        embeddings_bytes = io.BytesIO()
        np.save(embeddings_bytes, embeddings_float16)
        embeddings_data = embeddings_bytes.getvalue()

        data_offset = self._get_numpy_data_offset(embeddings_data)

        # Upload embeddings to S3
        self._client.put_object(
            Bucket=self.bucket, Key=embeddings_key, Body=embeddings_data
        )

        # Store shard metadata locally for later finalization
        shard_metadata = {
            "shard_id": shard_id,
            "filename": embeddings_filename,
            "n_samples": len(urls),
            "embedding_dim": embeddings.shape[1],
            "dtype": "float16",
            "shape": list(embeddings.shape),
            "data_offset": data_offset,
            "urls": urls,
        }
        self._shard_metadata.append(shard_metadata)

        logger.info(
            f"Stored shard {shard_id} with {len(urls)} embeddings (dim {embeddings.shape[1]}) to S3"
        )

    def write_metadata(self) -> None:
        """
        Finalize and write the combined metadata for all shards to S3.
        """
        if not self._shard_metadata:
            raise ValueError("No shard metadata to write. Call write_embeddings first.")

        # Sort shards by shard_id to ensure consistent ordering
        self._shard_metadata.sort(key=lambda x: x["shard_id"])

        # Calculate total samples and verify consistent embedding dimensions
        total_samples = sum(shard["n_samples"] for shard in self._shard_metadata)
        embedding_dim = self._shard_metadata[0]["embedding_dim"]

        # Verify all shards have the same embedding dimension
        if not all(
            shard["embedding_dim"] == embedding_dim for shard in self._shard_metadata
        ):
            raise ValueError("All shards must have the same embedding dimension")

        # Create combined metadata
        metadata = {
            "shards": self._shard_metadata,
            "total_samples": total_samples,
            "embedding_dim": embedding_dim,
            "n_shards": len(self._shard_metadata),
        }

        self._client.put_object(
            Bucket=self.bucket, Key=self._metadata_key, Body=json.dumps(metadata)
        )

        logger.info(
            f"Wrote metadata for {len(self._shard_metadata)} shards with {total_samples} total embeddings"
        )


class NpyReader:
    """
    For synchronous reading of numpy arrays from S3 with optional local caching.
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
        self._client = client or _get_client()

    def _get_shard_key(self, shard_filename: str) -> str:
        return f"{self.key_prefix}/{shard_filename}"

    def prewarm_cache(self) -> None:
        """
        Preload the local cache with the metadata and shards.
        """
        if not self._cache_locally:
            raise ValueError("Cannot prewarm cache without local cache directory")
        metadata_path = self._local_cache_dir / self._metadata_key
        if not metadata_path.exists():
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Preloading metadata to local cache")
            response = self._client.get_object(
                Bucket=self.bucket, Key=self._metadata_key
            )
            with open(metadata_path, "wb") as f:
                f.write(response["Body"].read())

        with open(metadata_path) as f:
            content = f.read()
            self._metadata_cache = json.loads(content)

        for shard in self._metadata_cache["shards"]:
            shard_key = self._get_shard_key(shard["filename"])
            shard_path = self._local_cache_dir / shard_key
            if not shard_path.exists():
                shard_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Preloading shard {shard['filename']} to local cache")
                response = self._client.get_object(Bucket=self.bucket, Key=shard_key)
                with open(shard_path, "wb") as f:
                    f.write(response["Body"].read())

    def check_exists(self) -> bool:
        """
        Check if the embeddings exist in S3 by checking for metadata file.
        """
        logger.info(
            f"Checking if embeddings exist in S3 at s3://{self.bucket}/{self._metadata_key}"
        )
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._metadata_key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def _get_metadata(self) -> dict[str, Any]:
        """Get metadata from S3 with caching."""
        if self._metadata_cache is None:
            if self._cache_locally:
                with open(self._local_cache_dir / self._metadata_key) as f:
                    content = f.read()
                    self._metadata_cache = json.loads(content)
            else:
                response = self._client.get_object(
                    Bucket=self.bucket, Key=self._metadata_key
                )
                self._metadata_cache = json.loads(
                    response["Body"].read().decode("utf-8")
                )
        return self._metadata_cache

    def _range_read(self, embeddings_key: str, start_byte: int, end_byte: int) -> bytes:
        """
        Read a range of bytes from S3.
        """
        if self._cache_locally:
            with open(self._local_cache_dir / embeddings_key, "rb") as f:
                f.seek(start_byte)
                return f.read(end_byte - start_byte + 1)
        else:
            response = self._client.get_object(
                Bucket=self.bucket,
                Key=embeddings_key,
                Range=f"bytes={start_byte}-{end_byte}",
            )
            return response["Body"].read()

    def _read_shard_chunk(
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

        chunk_bytes = self._range_read(embeddings_key, start_byte, end_byte)

        # Convert bytes back to numpy array
        return np.frombuffer(chunk_bytes, dtype=dtype).reshape(
            chunk_rows, embedding_dim
        )

    def get_sample_count(self) -> int:
        """Get total number of samples across all shards without loading data."""
        metadata = self._get_metadata()
        total_samples: int = metadata["total_samples"]
        return total_samples

    def read_embeddings_chunked(
        self, chunk_size: int = 1000
    ) -> Generator[tuple[np.ndarray, list[str]], None, None]:
        """
        Stream read embeddings in chunks from all shards using range requests.
        Transparently handles multiple shard files.

        Args:
            chunk_size: Number of samples to read per chunk

        Yields:
            Tuple of (embeddings_chunk, urls_chunk)
        """
        metadata = self._get_metadata()
        shards = metadata["shards"]

        for shard in shards:
            n_samples = shard["n_samples"]
            urls = shard["urls"]

            # Process this shard in chunks
            for start_idx in range(0, n_samples, chunk_size):
                end_idx = min(start_idx + chunk_size, n_samples)
                embeddings_chunk = self._read_shard_chunk(shard, start_idx, end_idx)
                urls_chunk = urls[start_idx:end_idx]
                yield embeddings_chunk, urls_chunk

    def read_embeddings_by_urls(self, urls: list[str]) -> tuple[np.ndarray, list[str]]:
        """
        Read embeddings for specific URLs using targeted byte range requests.

        Args:
            urls: List of S3 URLs to retrieve embeddings for

        Returns:
            Tuple of (embeddings, found_urls) where embeddings are in the same order as input URLs.
            Only URLs that exist in the storage will be returned.
        """
        metadata = self._get_metadata()
        shards = metadata["shards"]
        embedding_dim = metadata["embedding_dim"]

        # Create a mapping from URL to (shard_index, url_index)
        url_to_location = {}
        for shard_idx, shard in enumerate(shards):
            for url_idx, url in enumerate(shard["urls"]):
                url_to_location[url] = (shard_idx, url_idx)

        # Prepare results in the same order as input
        embeddings_list = []
        found_urls = []

        for url in urls:
            if url in url_to_location:
                shard_idx, url_idx = url_to_location[url]
                shard = shards[shard_idx]
                embedding = self._read_shard_chunk(shard, url_idx, url_idx + 1)
                embeddings_list.append(embedding)
                found_urls.append(url)

        if not embeddings_list:
            return np.array([]).reshape(0, embedding_dim), []

        # Combine all embeddings
        combined_embeddings = np.vstack(embeddings_list)
        return combined_embeddings, found_urls
