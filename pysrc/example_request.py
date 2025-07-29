import json
import pprint
import sys
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError


class Processor:


    def fail(self, msg):
        raise Exception(msg)

    def http_post_wm2(self, endpoint, payload, expected_status=200):
        """
        Helper method to make HTTP POST requests with JSON payloads and handle common error cases.

        Args:
            endpoint (str): The API endpoint to call
            payload (dict): The JSON payload to send in the request body
            expected_status (int): Expected HTTP status code

        Returns:
            tuple: (response_data, status_code, headers)
        """

        url = f"http://localhost:7777{endpoint}"

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

    def process_entry(self, text):
        req = {"doc": text}
        t0 = time.time()
        data, status_code, headers = self.http_post_wm2("/doc/labels", req)
        response = json.loads(data)
        elapsed = int((time.time() - t0) * 1000)

        context_page_titles = [p["page"]["title"] for p in response["context"]["pages"]]
        context_page_weights = [p["weight"] for p in response["context"]["pages"]]
        resolved_labels = []
        for l in response["resolvedLabels"]:
            e = {"label": l["label"], "pageId": l["page"]["id"], "pageTitle": l["page"]["title"]}
            resolved_labels.append(e)

        links = []
        for link in response["links"]:
            entry = link[1].copy()
            entry["title"] = link[0]["title"]
            if entry["linkPrediction"] > 0.25:
                links.append(entry)

        results = {
            "elapsed": elapsed,
            "text_size": len(text),
            "mined_labels": response["labels"],
            "resolved_labels": resolved_labels,
            "links": links,
            "context": list(zip(context_page_titles, context_page_weights))
        }

        return results


if __name__ == '__main__':
    with open(sys.argv[1]) as infile:
        text = infile.read()
        processor = Processor()
        result = processor.process_entry(text)

        pprint.pp(result)
