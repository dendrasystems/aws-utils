import json
import numpy as np
import pytest
from botocore.exceptions import ClientError

from aws_utils.npy_storage import NpyReader, NpyWriter


class TestNpyWriter:
    def test_write_single_shard_and_metadata(self, s3, test_bucket, embeddings_data):
        embeddings, urls = embeddings_data
        writer = NpyWriter(bucket=test_bucket, key_prefix="test_writer", client=s3)

        writer.write_embeddings(embeddings, urls, shard_id=0)
        writer.write_metadata()

        # Verify shard file
        try:
            s3.head_object(Bucket=test_bucket, Key="test_writer/embeddings_shard_0.npy")
        except ClientError:
            pytest.fail("Shard 0 embeddings file not found")

        # Verify metadata file
        try:
            response = s3.get_object(
                Bucket=test_bucket, Key="test_writer/embeddings_metadata.json"
            )
            metadata = json.loads(response["Body"].read())
        except ClientError:
            pytest.fail("Metadata file not found")

        assert metadata["total_samples"] == len(urls)
        assert metadata["embedding_dim"] == embeddings.shape[1]
        assert metadata["n_shards"] == 1
        assert len(metadata["shards"]) == 1
        assert metadata["shards"][0]["shard_id"] == 0
        assert metadata["shards"][0]["n_samples"] == len(urls)
        assert metadata["shards"][0]["dtype"] == "float16"

    def test_write_multiple_shards(self, s3, test_bucket, embeddings_data):
        embeddings, urls = embeddings_data
        shard_size = len(urls) // 2

        writer = NpyWriter(
            bucket=test_bucket, key_prefix="test_writer_multi", client=s3
        )

        # Shard 0
        writer.write_embeddings(embeddings[:shard_size], urls[:shard_size], shard_id=0)
        # Shard 1
        writer.write_embeddings(embeddings[shard_size:], urls[shard_size:], shard_id=1)

        writer.write_metadata()

        # Verify files
        s3.head_object(
            Bucket=test_bucket, Key="test_writer_multi/embeddings_shard_0.npy"
        )
        s3.head_object(
            Bucket=test_bucket, Key="test_writer_multi/embeddings_shard_1.npy"
        )
        response = s3.get_object(
            Bucket=test_bucket, Key="test_writer_multi/embeddings_metadata.json"
        )
        metadata = json.loads(response["Body"].read())

        assert metadata["total_samples"] == len(urls)
        assert metadata["n_shards"] == 2
        assert metadata["shards"][0]["n_samples"] == shard_size
        assert metadata["shards"][1]["n_samples"] == len(urls) - shard_size

    def test_metadata_error_on_no_shards(self, s3, test_bucket):
        writer = NpyWriter(
            bucket=test_bucket, key_prefix="test_writer_error", client=s3
        )
        with pytest.raises(ValueError, match="No shard metadata to write"):
            writer.write_metadata()


class TestNpyReader:
    KEY_PREFIX = "test_reader_sync"

    @pytest.fixture(autouse=True)
    def setup_s3_data(self, s3, test_bucket, embeddings_data):
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

        # Use the synchronous client directly
        self.reader = NpyReader(
            bucket=test_bucket, key_prefix=self.KEY_PREFIX, client=s3
        )
        yield

    def test_check_exists(self, s3, test_bucket):
        assert self.reader.check_exists() is True

        non_existent_reader = NpyReader(
            bucket=test_bucket, key_prefix="nonexistent", client=s3
        )
        assert non_existent_reader.check_exists() is False

    def test_get_sample_count(self):
        count = self.reader.get_sample_count()
        assert count == self.n_samples

    def test_read_embeddings_chunked(self):
        chunk_size = 25
        all_chunks_embeddings = []
        all_chunks_urls = []
        for chunk_embeddings, chunk_urls in self.reader.read_embeddings_chunked(
            chunk_size
        ):
            all_chunks_embeddings.append(chunk_embeddings)
            all_chunks_urls.extend(chunk_urls)

        assert len(all_chunks_urls) == self.n_samples
        reconstructed_embeddings = np.vstack(all_chunks_embeddings)
        expected_embeddings = self.embeddings.astype(np.float16)
        np.testing.assert_array_equal(reconstructed_embeddings, expected_embeddings)
        assert all_chunks_urls == self.urls

    def test_read_embeddings_by_urls(self):
        urls_to_fetch = [self.urls[5], self.urls[25], self.urls[75]]
        embeddings, found_urls = self.reader.read_embeddings_by_urls(urls_to_fetch)

        assert found_urls == urls_to_fetch
        assert embeddings.shape == (len(urls_to_fetch), self.embedding_dim)

        expected_embeddings = np.array(
            [self.embeddings[5], self.embeddings[25], self.embeddings[75]]
        ).astype(np.float16)
        np.testing.assert_array_equal(embeddings, expected_embeddings)

    def test_read_embeddings_by_urls_not_found(self):
        urls_to_fetch = ["s3://non-existent/url.jpg"]
        embeddings, found_urls = self.reader.read_embeddings_by_urls(urls_to_fetch)
        assert len(found_urls) == 0
        assert embeddings.shape == (0, self.embedding_dim)

    def test_prewarm_cache_without_cache_dir(self, s3, test_bucket):
        reader = NpyReader(bucket=test_bucket, key_prefix=self.KEY_PREFIX, client=s3)
        with pytest.raises(
            ValueError, match="Cannot prewarm cache without local cache directory"
        ):
            reader.prewarm_cache()

    def test_caching(self, tmp_path, s3, test_bucket):
        # Create a reader with a cache directory
        cached_reader = NpyReader(
            bucket=test_bucket,
            key_prefix=self.KEY_PREFIX,
            cache_dir=str(tmp_path),
            client=s3,
        )

        # Prewarm the cache
        cached_reader.prewarm_cache()

        # Check that metadata and shard files are cached
        metadata_path = tmp_path / self.KEY_PREFIX / "embeddings_metadata.json"
        shard_0_path = tmp_path / self.KEY_PREFIX / "embeddings_shard_0.npy"
        shard_1_path = tmp_path / self.KEY_PREFIX / "embeddings_shard_1.npy"
        assert metadata_path.exists()
        assert shard_0_path.exists()
        assert shard_1_path.exists()

        # Read from cache
        count = cached_reader.get_sample_count()
        assert count == self.n_samples

        all_chunks_embeddings = []
        for chunk_embeddings, _ in cached_reader.read_embeddings_chunked(chunk_size=50):
            all_chunks_embeddings.append(chunk_embeddings)

        reconstructed = np.vstack(all_chunks_embeddings)
        np.testing.assert_array_equal(reconstructed, self.embeddings.astype(np.float16))
