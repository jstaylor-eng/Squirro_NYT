import logging
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

NYT_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json?"


class NYTimesSource:
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(
        self, api_key, query, max_retries=5, inc_column=None, max_inc_value=None
    ):
        self.api_key = api_key
        self.query = query
        self.max_retries = max_retries
        self.inc_column = inc_column
        self.max_inc_value = max_inc_value
        self.schema = set()
        self.session = requests.Session()

    def connect(self, inc_column=None, max_inc_value=None):
        """Connect to the source"""
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        log.info("Disconnected from source")
        self.session.close()
        self.session = None

    def _fetch_data(self, page=0):
        """Internal use function to fetch data via NYT API, and manage retries."""
        attempt = 0
        params = {
            "q": self.query,
            "api-key": self.api_key,
            "page": page,
            "sort": "newest",
        }

        while attempt < self.max_retries:
            try:
                response = self.session.get(NYT_URL, params=params)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    log.warning("Invalid API key")
                    break
                elif response.status_code == 429:
                    log.warning("Rate limit exceeded, waiting 20 seconds before retry")
                    time.sleep(20)
                    attempt += 1
                    continue
                else:
                    log.error("Error fetching data: %s", response.text)
                    return None
            except requests.RequestException as e:
                log.error("Request failed (attempt %d): %s", attempt + 1, str(e))

            attempt += 1
            time.sleep(2**attempt)

        log.error("Max retries reached or API key error, request skipped")
        return None

    def _flatten_dict(self, d, parent_key="", sep="."):
        """Internal use function to flatten dictionaries within NYT API response"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            # Unsure if wanting to completely flatten everything, or just dictionary levels.
            # If so, uncomment below block to enablbe complete flattening of lists and nested dictionaries.
            # elif isinstance(v, list):
            #     for idx, item in enumerate(v):
            #         if isinstance(item, dict):
            #             items.extend(
            #                 self._flatten_dict(item, f"{new_key}[{idx}]", sep).items()
            #             )
            #         else:
            #             items.append((f"{new_key}[{idx}]", item))
            else:
                items.append((new_key, v))
        return dict(items)

    def getSchema(self):
        return self.schema

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source in batches.
        :returns: One list for each batch. Each of those is a list of dictionaries with the defined rows.
        """
        page = 0
        batch = []
        while True:
            data = self._fetch_data(page)
            if not data or "response" not in data or "docs" not in data["response"]:
                log.debug("No more data to fetch or error in response.")
                break

            articles = data["response"]["docs"]
            if not articles:
                log.debug("No articles found in the response.")
                break

            for article in articles:
                flattened_article = self._flatten_dict(article)
                article_value = None
                # Skip already loaded articles
                if self.inc_column and self.max_inc_value:
                    article_value = flattened_article.get(self.inc_column)
                    if article_value and article_value <= self.max_inc_value:
                        continue

                batch.append(flattened_article)

                if self.inc_column and article_value:
                    self.max_inc_value = max(self.max_inc_value, article_value)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            page += 1
            time.sleep(1)

        if batch:  # Yield any remaining articles in the batch
            yield batch


if __name__ == "__main__":
    config = {
        "api_key": os.getenv("API_KEY"),
        "query": "bugti",
        "inc_column": "pub_date",
        "max_inc_value": None,
    }

    source = NYTimesSource(**config)
    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    # source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(12)):
        print(f"Batch {idx + 1} containing {len(batch)} items")
        for item in batch:
            print(
                f" - {item.get('_id', 'N/A')} - {item.get('headline.main', 'No Headline')}"
            )
        # print(batch)

        # print(f"Schema after batch {idx + 1}: ", source.getSchema())

    source.disconnect()
