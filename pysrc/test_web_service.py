import json
import unittest
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError


class WikiServiceIntegrationTest(unittest.TestCase):
    """Integration tests for the WebService HTTP endpoints."""

    BASE_URL = "http://localhost:7777"

    def http_get(self, endpoint, expected_status=200):
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

    def http_post(self, endpoint, payload, expected_status=200):
        """
        Helper method to make HTTP POST requests with JSON payloads and handle common error cases.

        Args:
            endpoint (str): The API endpoint to call
            payload (dict): The JSON payload to send in the request body
            expected_status (int): Expected HTTP status code

        Returns:
            tuple: (response_data, status_code, headers)
        """
        url = f"{self.BASE_URL}{endpoint}"

        try:
            # Convert payload to JSON bytes
            json_data = json.dumps(payload).encode('utf-8')

            # Create request with JSON content type header
            request = urllib.request.Request(
                url,
                data=json_data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(request) as response:
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
        data, status_code, headers = self.http_get("/ping")

        # Verify response
        self.assertEqual(status_code, 200, "Ping endpoint should return 200 OK")
        self.assertTrue(data.startswith("PONG"), "Ping endpoint should return 'PONG'")

    def test_ping_endpoint(self):
        """Test the /ping endpoint returns PONG with correct content type."""
        data, status_code, headers = self.http_get("/ping")

        # Verify response
        self.assertEqual(status_code, 200, "Ping endpoint should return 200 OK")
        self.assertTrue(data.startswith("PONG"), "Ping endpoint should return 'PONG'")

    def test_doc_labels_1(self):
        """Test processing a very short document."""
        text = """Mercury (Latin: Mercurius) is the god of trade, trickery, merchants and thieves."""
        req = {"doc": text}
        data, status_code, headers = self.http_post("/doc/labels", req)
        deserialized = json.loads(data)
        context_page_titles = [p["page"]["title"] for p in deserialized["context"]["pages"]]
        sample_titles = ["Latin", "Mercury (mythology)", "Mercury (planet)", "God", "Deity"]
        for t in sample_titles:
            self.assertTrue(t in context_page_titles)

    def test_get_article_by_page_id_success(self):
        """Test retrieving an article by page ID."""
        test_page_id = 12240
        endpoint = f"/wiki/page_id/{test_page_id}"
        data, status_code, headers = self.http_get(endpoint)

        # Verify response
        self.assertEqual(status_code, 200, f"Page ID endpoint should return 200 OK for ID {test_page_id}")
        try:
            json_data = json.loads(data)
            self.assertEqual(json_data["id"], test_page_id)
        except json.JSONDecodeError:
            self.fail(f"Response is not valid JSON: {data}")

        # Verify content type (if set)
        content_type = headers.get('content-type', '')
        if content_type:
            self.assertIn('json', content_type.lower(), "Content type should indicate JSON")

    def test_get_article_by_page_title_success(self):
        """Test retrieving an article by page title."""
        test_page_title = "Mercury (planet)"
        # URL encode the page title to handle spaces and special characters
        encoded_title = urllib.parse.quote(test_page_title, safe='')
        endpoint = f"/wiki/page_title/{encoded_title}"
        data, status_code, headers = self.http_get(endpoint)

        # Verify response
        self.assertEqual(status_code, 200, f"Page title endpoint should return 200 OK for title '{test_page_title}'")

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

        data, status_code, headers = self.http_get(endpoint, expected_status=404)
        self.assertEqual(status_code, 404, "Invalid page ID should return 404")

    def test_get_article_by_invalid_page_title(self):
        """Test retrieving an article with an invalid page title."""
        invalid_title = "NonExistentPageTitle12345"
        encoded_title = urllib.parse.quote(invalid_title, safe='')
        endpoint = f"/wiki/page_title/{encoded_title}"

        # Similar to invalid page ID test
        data, status_code, headers = self.http_get(endpoint, expected_status=404)
        self.assertEqual(status_code, 404, "Invalid page ID should return 404")

    def test_service_availability(self):
        """Test that the service is running and accessible."""
        try:
            self.http_get("/ping")
        except Exception as e:
            self.fail(f"Service is not accessible at {self.BASE_URL}. Make sure the WebService is running on port 7777. Error: {e}")

    def test_page_id_and_title_return_same_data(self):
        """Test that querying by page ID and title returns the same article data."""
        # Get data by page ID
        test_page_id=12240
        page_id_endpoint = f"/wiki/page_id/{test_page_id}"
        id_data, id_status, _ = self.http_get(page_id_endpoint)
        page = json.loads(id_data)

        # Get data by page title
        encoded_title = urllib.parse.quote(page["title"], safe='')
        title_endpoint = f"/wiki/page_title/{encoded_title}"
        title_data, title_status, _ = self.http_get(title_endpoint)

        # Both should succeed
        self.assertEqual(id_status, 200, "Page ID request should succeed")
        self.assertEqual(title_status, 200, "Page title request should succeed")
        self.assertEqual(json.loads(title_data), page)

    def test_doc_labels(self):
        """Test processing a small document for labels/context."""
        text = "So what did the people I asked know about the war? Nobody could tell me the first thing about it. Once they got past who won they almost drew a blank. All they knew were those big totemic names -- Pearl Harbor, D-Day, Auschwitz, Hiroshima -- whose unfathomable reaches of experience had been boiled down to an abstract atrocity. The rest was gone. Kasserine, Leyte Gulf, Corregidor, Falaise, the Ardennes didn't provoke a glimmer of recognition; they might as well have been off-ramps on some exotic interstate. I started getting the creepy feeling that the war had actually happened a thousand years ago, and so it was forgivable if people were a little vague on the difference between the Normandy invasion and the Norman Conquest and couldn't say offhand whether the boats sailed from France to England or the other way around."
        req = {"doc": text}

        data, status_code, headers = self.http_post("/doc/labels", req)
        deserialized = json.loads(data)

        # A sampling of expected context pages
        expected_context_titles = ["Normandy landings",
                                   "Norman Conquest",
                                   "France",
                                   "Attack on Pearl Harbor",
                                   "D-Day (military term)",
                                   "World War II",
                                   "Corregidor",
                                   "Operation Overlord"]
        context_page_titles = [p["page"]["title"] for p in deserialized["context"]["pages"]]
        for title in expected_context_titles:
            self.assertTrue(title in context_page_titles)


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)