import datetime
import os
from unittest import mock
import pytest

from aws_utils import s3
from botocore.exceptions import ClientError


class TestParseS3Url:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("s3://bucket/key", s3.S3UrlParts("bucket", "key")),
            ("https://bucket.s3.amazonaws.com/key", s3.S3UrlParts("bucket", "key")),
            (
                "https://s3-region.amazonaws.com/bucket/key",
                s3.S3UrlParts("bucket", "key"),
            ),
        ],
    )
    def test_valid(self, url, expected):
        assert s3.parse_s3_url(url) == expected

    @pytest.mark.parametrize(
        "url",
        [
            "https://google.com",
            "ftp://example.com",
        ],
    )
    def test_invalid(self, url):
        with pytest.raises(ValueError):
            s3.parse_s3_url(url)


class TestMakeS3Url:
    def test_func(self):
        assert (
            s3.make_s3_url(bucket="test", key="foo/bar.jpg") == "s3://test/foo/bar.jpg"
        )


class TestMakeHttpsUrl:
    def test_func(self):
        assert (
            s3.make_https_url(bucket="test", key="foo/bar.jpg", aws_region="eu-west-1")
            == "https://test.s3.eu-west-1.amazonaws.com/foo/bar.jpg"
        )


class TestIterKeys:
    def test_no_keys_found(self):
        mock_client = mock.MagicMock()
        mock_client.list_objects_v2.return_value = {"KeyCount": 0}
        generator = s3.iter_keys(bucket="test", prefix="images/", client=mock_client)
        assert list(generator) == []

    def test_untruncated_response(self):
        mock_client = mock.MagicMock()
        mock_client.list_objects_v2.return_value = {
            "KeyCount": 10,
            "IsTruncated": False,
            "Contents": [{}] * 10,
        }
        generator = s3.iter_keys(bucket="test", prefix="images/", client=mock_client)
        assert len(list(generator)) == 10

    def test_truncated_response_is_fetched_in_pages(self):
        mock_client = mock.MagicMock()
        mock_client.list_objects_v2.side_effect = [
            {
                "KeyCount": 10,
                "IsTruncated": True,
                "NextContinuationToken": "next-token",
                "Contents": [{}] * 10,
            },
            {"KeyCount": 10, "IsTruncated": False, "Contents": [{}] * 5},
        ]
        generator = s3.iter_keys(bucket="test", prefix="images/", client=mock_client)
        assert len(list(generator)) == 15
        assert mock_client.list_objects_v2.call_count == 2

    def test_limits_results__max_keys(self):
        mock_client = mock.MagicMock()
        mock_client.list_objects_v2.side_effect = [
            {
                "KeyCount": 10,
                "IsTruncated": True,
                "NextContinuationToken": "next-token",
                "Contents": [{}] * 10,
            },
            {"KeyCount": 10, "IsTruncated": False, "Contents": [{}] * 5},
        ]
        generator = s3.iter_keys(
            bucket="test", prefix="images/", max_keys=11, client=mock_client
        )
        assert len(list(generator)) == 11


class TestSyncObject:
    def test_copy_dest_does_not_exist(self):
        mock_client = mock.MagicMock()
        mock_client.head_object.side_effect = [
            ClientError(error_response={}, operation_name="HeadObject"),
            {"LastModified": datetime.datetime(2015, 1, 1)},
        ]
        s3.sync_object(
            {"Bucket": "src", "Key": "item.png"},
            {"Bucket": "dest", "Key": "item.png"},
            client=mock_client,
        )
        mock_client.copy.assert_called_once_with(
            CopySource={"Bucket": "src", "Key": "item.png"},
            Bucket="dest",
            Key="item.png",
        )

    def test_copy_dest_older_than_src(self):
        mock_client = mock.MagicMock()
        mock_client.head_object.side_effect = [
            {"LastModified": datetime.datetime(2015, 1, 1)},
            {"LastModified": datetime.datetime(2016, 1, 1)},
        ]
        s3.sync_object(
            {"Bucket": "src", "Key": "item.png"},
            {"Bucket": "dest", "Key": "item.png"},
            client=mock_client,
        )
        mock_client.copy.assert_called_once_with(
            CopySource={"Bucket": "src", "Key": "item.png"},
            Bucket="dest",
            Key="item.png",
        )

    def test_copy_dest_newer_than_src(self):
        mock_client = mock.MagicMock()
        mock_client.head_object.side_effect = [
            {"LastModified": datetime.datetime(2016, 1, 1)},
            {"LastModified": datetime.datetime(2015, 1, 1)},
        ]
        s3.sync_object(
            {"Bucket": "src", "Key": "item.png"},
            {"Bucket": "dest", "Key": "item.png"},
            client=mock_client,
        )
        mock_client.copy.assert_not_called()


class TestUploadFile:
    def test_sets_extra_args(self):
        mock_client = mock.MagicMock()
        s3.upload_file(
            source_path="some/file.png",
            bucket="test",
            key="output/file.png",
            content_type="image/png",
            content_disposition="attachment",
            client=mock_client,
        )
        mock_client.upload_file.assert_called_once_with(
            "some/file.png",
            Bucket="test",
            Key="output/file.png",
            ExtraArgs={"ContentType": "image/png", "ContentDisposition": "attachment"},
        )


class TestUploadDir:
    def test_upload_call(self, mocker):
        mocker.patch.object(
            os,
            "walk",
            return_value=[
                ("./src", ["dir1", "dir2"], []),
                ("./src/dir1", [], ["file1.txt", "file2.txt"]),
                ("./src/dir2", [], ["file3.txt"]),
            ],
        )
        mock_manager = mock.MagicMock()
        mocker.patch.object(
            s3.transfer, "create_transfer_manager", return_value=mock_manager
        )
        s3.upload_dir("./src", "test", "output/")

        assert mock_manager.upload.call_count == 3
        mock_manager.upload.assert_has_calls(
            [
                mock.call("./src/dir1/file1.txt", "test", "output/dir1/file1.txt"),
                mock.call("./src/dir1/file2.txt", "test", "output/dir1/file2.txt"),
                mock.call("./src/dir2/file3.txt", "test", "output/dir2/file3.txt"),
            ]
        )
        mock_manager.shutdown.assert_called_once()
