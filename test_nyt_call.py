import unittest
from unittest.mock import patch, Mock, MagicMock
import requests
from nyt_call import NYTimesSource


class TestNYTimesSource(unittest.TestCase):

    def setUp(self):
        self.api_key = "test_api_key"
        self.query = "test_query"
        self.source = NYTimesSource(api_key=self.api_key, query=self.query)

    def test_initialization(self):
        self.assertEqual(self.source.api_key, self.api_key)
        self.assertEqual(self.source.query, self.query)
        self.assertEqual(self.source.max_retries, 5)
        self.assertIsNone(self.source.inc_column)
        self.assertIsNone(self.source.max_inc_value)
        self.assertEqual(self.source.schema, set())
        self.assertIsInstance(self.source.session, requests.Session)

    @patch("nyt_call.log")
    def test_connect(self, mock_log):
        self.source.connect(inc_column="test_column", max_inc_value="test_value")
        mock_log.debug.assert_any_call("Incremental Column: %r", "test_column")
        mock_log.debug.assert_any_call("Incremental Last Value: %r", "test_value")

    @patch("nyt_call.requests.Session.get")
    @patch("nyt_call.log")
    def test_invalid_api_key(self, mock_log, mock_get):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        source = NYTimesSource(api_key="invalid_key", query="test")
        data = source._fetch_data()

        self.assertIsNone(data)
        mock_log.warning.assert_called_with("Invalid API key")

    def test_flatten_dict(self):
        nested_dict = {"a": {"b": {"c": 1}}, "d": 2}
        flattened = self.source._flatten_dict(nested_dict)
        self.assertEqual(flattened, {"a.b.c": 1, "d": 2})

    @patch("nyt_call.requests.Session.get")
    def test_fetch_data_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "response": {
                "docs": [
                    {"headline": {"main": "Test Headline 1"}, "_id": "1"},
                    {"headline": {"main": "Test Headline 2"}, "_id": "2"},
                ]
            }
        }
        mock_get.return_value = mock_response

        data = self.source._fetch_data()

        self.assertIsNotNone(data)
        self.assertIn("response", data)
        self.assertIn("docs", data["response"])
        self.assertEqual(len(data["response"]["docs"]), 2)
        self.assertEqual(
            data["response"]["docs"][0]["headline"]["main"], "Test Headline 1"
        )
        self.assertEqual(
            data["response"]["docs"][1]["headline"]["main"], "Test Headline 2"
        )

    @unittest.skip("Placeholder test")
    def test_fetch_data_rate_limit(self):
        pass

    @unittest.skip("Placeholder test")
    def test_fetch_data_error(self):
        pass

    @unittest.skip("Placeholder test")
    @patch("nyt_call.requests.Session.get")
    def test_getDataBatch(self):
        pass

    @unittest.skip("Placeholder test")
    def test_fetch_data_retry(self):
        pass

    @unittest.skip("Placeholder test")
    def test_fetch_data_request_exception(self):
        pass

    @unittest.skip("Placeholder test")
    def test_disconnect(self):
        pass


if __name__ == "__main__":
    unittest.main()
