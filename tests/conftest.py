import numpy as np
import boto3
import pytest
from moto import mock_aws


@pytest.fixture(scope="function")
def s3():
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def test_bucket(s3):
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name


@pytest.fixture(scope="module")
def embeddings_data():
    n_samples = 100
    embedding_dim = 128
    np.random.seed(42)
    embeddings = np.random.random((n_samples, embedding_dim)).astype(np.float32)
    urls = [f"s3://test-bucket/images/tile_{i:04d}.jpg" for i in range(n_samples)]
    return embeddings, urls
