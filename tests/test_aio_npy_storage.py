import numpy as np
import pytest
import pytest_asyncio

from aws_utils.npy_storage import NpyWriter
from aws_utils.aio_npy_storage import AIONpyReader


class MockAsyncS3Client:
    """
    moto3 does not support async clients, so we need to wrap the sync client in an async client.
    """

    def __init__(self, sync_client):
        self.sync_client = sync_client
        self.exceptions = sync_client.exceptions

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def head_object(self, **kwargs):
        return self.sync_client.head_object(**kwargs)

    async def get_object(self, **kwargs):
        response = self.sync_client.get_object(**kwargs)
        # Wrap the body to provide async interface
        response["Body"] = MockAsyncBody(response["Body"])
        return response

    async def download_file(self, Bucket, Key, Filename):
        self.sync_client.download_file(Bucket=Bucket, Key=Key, Filename=Filename)


class MockAsyncBody:
    def __init__(self, sync_body):
        self.sync_body = sync_body

    async def read(self):
        return self.sync_body.read()


@pytest.mark.asyncio
class TestAIONpyReader:
    KEY_PREFIX = "test_reader"

    @pytest_asyncio.fixture(autouse=True)
    async def setup_s3_data(self, s3, test_bucket, embeddings_data):
        """Use NpyWriter to set up S3 data for reader tests."""
        writer = NpyWriter(bucket=test_bucket, key_prefix=self.KEY_PREFIX, client=s3)
        embeddings, urls = embeddings_data
        shard_size = len(urls) // 2
        shard_0_embeddings = embeddings[:shard_size]
        shard_0_urls = urls[:shard_size]
        shard_1_embeddings = embeddings[shard_size:]
        shard_1_urls = urls[shard_size:]
        writer.write_embeddings(shard_0_embeddings, shard_0_urls, shard_id=0)
        writer.write_embeddings(shard_1_embeddings, shard_1_urls, shard_id=1)
        writer.write_metadata()

        self.embeddings = embeddings
        self.urls = urls
        self.embedding_dim = embeddings.shape[1]
        self.n_samples = len(urls)

        # Use the mock async client wrapper
        mock_client = MockAsyncS3Client(s3)
        self.reader = AIONpyReader(
            bucket=test_bucket, key_prefix=self.KEY_PREFIX, client=mock_client
        )
        yield

    async def test_check_exists(self, s3, tmp_path, test_bucket):
        mock_client = MockAsyncS3Client(s3)
        assert await self.reader.check_exists() is True

        non_existent_reader = AIONpyReader(
            bucket=test_bucket, key_prefix="nonexistent", client=mock_client
        )
        assert await non_existent_reader.check_exists() is False

        cached_reader = AIONpyReader(
            bucket=test_bucket,
            key_prefix="something_completely_different",
            client=mock_client,
            cache_dir=str(tmp_path),
        )
        meta_path = tmp_path / cached_reader._metadata_key
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_bytes(b"{}")  # minimal valid JSON

        assert await cached_reader.check_exists() is True

    async def test_get_sample_count(self):
        count = await self.reader.get_sample_count()
        assert count == self.n_samples

    async def test_read_embeddings_chunked(self):
        chunk_size = 25
        all_chunks_embeddings = []
        all_chunks_urls = []
        async for chunk_embeddings, chunk_urls in self.reader.read_embeddings_chunked(
            chunk_size
        ):
            all_chunks_embeddings.append(chunk_embeddings)
            all_chunks_urls.extend(chunk_urls)

        assert len(all_chunks_urls) == self.n_samples
        reconstructed_embeddings = np.vstack(all_chunks_embeddings)
        expected_embeddings = self.embeddings.astype(np.float16)
        np.testing.assert_array_equal(reconstructed_embeddings, expected_embeddings)
        assert all_chunks_urls == self.urls

    async def test_read_embeddings_by_urls(self):
        urls_to_fetch = [self.urls[5], self.urls[25], self.urls[75]]
        embeddings, found_urls = await self.reader.read_embeddings_by_urls(
            urls_to_fetch
        )

        assert found_urls == urls_to_fetch
        assert embeddings.shape == (len(urls_to_fetch), self.embedding_dim)

        expected_embeddings = np.array(
            [self.embeddings[5], self.embeddings[25], self.embeddings[75]]
        ).astype(np.float16)
        np.testing.assert_array_equal(embeddings, expected_embeddings)

    async def test_read_embeddings_by_urls_not_found(self):
        urls_to_fetch = ["s3://non-existent/url.jpg"]
        embeddings, found_urls = await self.reader.read_embeddings_by_urls(
            urls_to_fetch
        )
        assert len(found_urls) == 0
        assert embeddings.shape == (0, self.embedding_dim)

    async def test_prewarm_cache_without_cache_dir(self, s3, test_bucket):
        mock_client = MockAsyncS3Client(s3)
        reader = AIONpyReader(
            bucket=test_bucket, key_prefix=self.KEY_PREFIX, client=mock_client
        )
        with pytest.raises(
            ValueError, match="Cannot prewarm cache without local cache directory"
        ):
            await reader.prewarm_cache()

    async def test_caching(self, tmp_path, s3, test_bucket):
        mock_client = MockAsyncS3Client(s3)
        # Create a reader with a cache directory
        cached_reader = AIONpyReader(
            bucket=test_bucket,
            key_prefix=self.KEY_PREFIX,
            cache_dir=str(tmp_path),
            client=mock_client,
        )

        # Prewarm the cache
        await cached_reader.prewarm_cache()

        # Check that metadata and shard files are cached
        metadata_path = tmp_path / self.KEY_PREFIX / "embeddings_metadata.json"
        shard_0_path = tmp_path / self.KEY_PREFIX / "embeddings_shard_0.npy"
        shard_1_path = tmp_path / self.KEY_PREFIX / "embeddings_shard_1.npy"
        assert metadata_path.exists()
        assert shard_0_path.exists()
        assert shard_1_path.exists()

        # Read from cache
        count = await cached_reader.get_sample_count()
        assert count == self.n_samples

        all_chunks_embeddings = []
        async for chunk_embeddings, _ in cached_reader.read_embeddings_chunked(
            chunk_size=50
        ):
            all_chunks_embeddings.append(chunk_embeddings)

        reconstructed = np.vstack(all_chunks_embeddings)
        np.testing.assert_array_equal(reconstructed, self.embeddings.astype(np.float16))
