import json
import unittest
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError


class WikiServiceIntegrationTest(unittest.TestCase):
    """Integration tests for the Scala Wiki service HTTP endpoints."""

    BASE_URL = "http://localhost:7777"
    TEST_PAGE_ID = 12240
    TEST_PAGE_TITLE = "Mercury (planet)"

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.maxDiff = None  # Show full diff for assertion failures

    def _make_request(self, endpoint, expected_status=200):
        """
        Helper method to make HTTP requests and handle common error cases.

        Args:
            endpoint (str): The API endpoint to call
            expected_status (int): Expected HTTP status code

        Returns:
            tuple: (response_data, status_code, headers)
        """
        url = f"{self.BASE_URL}{endpoint}"

        try:
            with urllib.request.urlopen(url) as response:
                data = response.read().decode('utf-8')
                return data, response.getcode(), dict(response.headers)
        except HTTPError as e:
            if e.code == expected_status:
                data = e.read().decode('utf-8') if e.fp else ""
                return data, e.code, dict(e.headers)
            else:
                self.fail(f"Unexpected HTTP error {e.code} for {url}: {e.reason}")
        except URLError as e:
            self.fail(f"Failed to connect to service at {url}: {e.reason}")

    def test_ping_endpoint(self):
        """Test the /ping endpoint returns PONG with correct content type."""
        data, status_code, headers = self._make_request("/ping")

        # Verify response
        self.assertEqual(status_code, 200, "Ping endpoint should return 200 OK")
        self.assertTrue(data.startswith("PONG"), "Ping endpoint should return 'PONG'")

    def test_get_article_by_page_id_success(self):
        """Test retrieving an article by page ID."""
        endpoint = f"/wiki/page_id/{self.TEST_PAGE_ID}"
        data, status_code, headers = self._make_request(endpoint)

        # Verify response
        self.assertEqual(status_code, 200, f"Page ID endpoint should return 200 OK for ID {self.TEST_PAGE_ID}")
        try:
            json_data = json.loads(data)
            self.assertEqual(json_data["id"], self.TEST_PAGE_ID)
        except json.JSONDecodeError:
            self.fail(f"Response is not valid JSON: {data}")

        # Verify content type (if set)
        content_type = headers.get('content-type', '')
        if content_type:
            self.assertIn('json', content_type.lower(), "Content type should indicate JSON")

    def test_get_article_by_page_title_success(self):
        """Test retrieving an article by page title."""
        # URL encode the page title to handle spaces and special characters
        encoded_title = urllib.parse.quote(self.TEST_PAGE_TITLE, safe='')
        endpoint = f"/wiki/page_title/{encoded_title}"
        data, status_code, headers = self._make_request(endpoint)

        # Verify response
        self.assertEqual(status_code, 200, f"Page title endpoint should return 200 OK for title '{self.TEST_PAGE_TITLE}'")

        # Verify it's valid JSON
        try:
            json_data = json.loads(data)
            self.assertIsInstance(json_data, (dict, list), "Response should be valid JSON")
        except json.JSONDecodeError:
            self.fail(f"Response is not valid JSON: {data}")

        # Verify content type (if set)
        content_type = headers.get('content-type', '')
        if content_type:
            self.assertIn('json', content_type.lower(), "Content type should indicate JSON")

    def test_get_article_by_invalid_page_id(self):
        """Test retrieving an article with an invalid page ID."""
        invalid_page_id = 2147483647
        endpoint = f"/wiki/page_id/{invalid_page_id}"

        data, status_code, headers = self._make_request(endpoint, expected_status=404)
        self.assertEqual(status_code, 404, "Invalid page ID should return 404")

    def test_get_article_by_invalid_page_title(self):
        """Test retrieving an article with an invalid page title."""
        invalid_title = "NonExistentPageTitle12345"
        encoded_title = urllib.parse.quote(invalid_title, safe='')
        endpoint = f"/wiki/page_title/{encoded_title}"

        # Similar to invalid page ID test
        data, status_code, headers = self._make_request(endpoint, expected_status=404)
        self.assertEqual(status_code, 404, "Invalid page ID should return 404")

    def test_service_availability(self):
        """Test that the service is running and accessible."""
        try:
            self._make_request("/ping")
        except Exception as e:
            self.fail(f"Service is not accessible at {self.BASE_URL}. Make sure the Scala service is running on port 7777. Error: {e}")

    def xtest_page_id_and_title_return_same_data(self):
        """Test that querying by page ID and title returns the same article data."""
        # Get data by page ID
        page_id_endpoint = f"/wiki/page_id/{self.TEST_PAGE_ID}"
        id_data, id_status, _ = self._make_request(page_id_endpoint)

        # Get data by page title
        encoded_title = urllib.parse.quote(self.TEST_PAGE_TITLE, safe='')
        title_endpoint = f"/wiki/page_title/{encoded_title}"
        title_data, title_status, _ = self._make_request(title_endpoint)

        # Both should succeed
        self.assertEqual(id_status, 200, "Page ID request should succeed")
        self.assertEqual(title_status, 200, "Page title request should succeed")

        # Parse JSON responses
        try:
            id_json = json.loads(id_data)
            title_json = json.loads(title_data)

            # The data should be the same (this assumes they refer to the same article)
            self.assertEqual(
                id_json,
                title_json,
                f"Article data for ID {self.TEST_PAGE_ID} and title '{self.TEST_PAGE_TITLE}' should be identical"
            )
        except json.JSONDecodeError as e:
            self.fail(f"Failed to parse JSON responses: {e}")


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
