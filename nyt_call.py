# import argparse
import logging
import requests
import time
import os

# import pprint

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

    def connect(self, inc_column=None, max_inc_value=None):
        """Connect to the source"""
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        pass

    def _get_data(self, page=0):
        attempt = 0
        params = {
            "q": self.query,
            "api-key": self.api_key,
            "page": page,
            "sort": "newest",
        }

        while attempt < self.max_retries:
            try:
                # log.info("Attempting url: ", NYT_URL, params)  # check for working url
                response = requests.get(NYT_URL, params=params)
                if response.status_code == 200:
                    # pprint.pprint(response.text) # raw response check (import pprint)
                    return response.json()
                else:
                    log.error("Error fetching data: %s", response.text)
            except requests.RequestException as e:
                log.error("Request failed (attempt %d): %s", attempt + 1, str(e))

            attempt += 1
            time.sleep(2**attempt)

        log.error("Max retries reached, request skipped")
        return None

    def _flatten_dict(self, d, parent_key="", sep="."):
        items = []
        # loop through keys and values
        for k, v in d.items():
            # if nested dict, create new key with "." between k and v
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                # if value is dict, flatten (recursivly) and append to items.
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
            else:  # else append key and value to items list
                items.append((new_key, v))
        # pprint.pprint(dict(items)) # prints flattend dictonary to console.
        return dict(items)

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.
        :returns: One list for each batch. Each of those is a list of dictionaries with the defined rows.
        """
        page = 0
        while True:
            data = self._get_data(page)
            if not data or "response" not in data or "docs" not in data["response"]:
                break

            articles = data["response"]["docs"]
            if not articles:
                break

            batch = (
                []
            )  # originally [self._flatten_dict(article) for article in articles]
            for article in articles:
                flattened_article = self._flatten_dict(article)
                article_value = None
                # filter already loaded articles
                if self.inc_column and self.max_inc_value:
                    article_value = flattened_article.get(self.inc_column)
                    if article_value and article_value <= self.max_inc_value:
                        continue
                # append flattend article to batch
                batch.append(flattened_article)
                # set the maximum incriment to highest of existing max or current article
                if self.inc_column and article_value:
                    self.max_inc_value = max(self.max_inc_value, article_value)

                if len(batch) >= batch_size:
                    break

            self.schema.update(key for article in batch for key in article.keys())
            yield batch

            page += 1
            time.sleep(1)
            if len(batch) < batch_size:
                break

            # TODO: implement - this dummy implementation returns one batch of data
            # yield [
            #     {
            #         "headline.main": "The main headline",
            #         "_id": "1234",
            #     }
            # ]

    def getSchema(self):
        # print(sorted(self.schema))
        return self.schema


if __name__ == "__main__":
    config = {
        "api_key": os.getenv("API_KEY"),
        "query": "Silicon Valley",
        "inc_column": "pub_date",
        "max_inc_value": None,
    }

    source = NYTimesSource(**config)
    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    # source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(5)):
        print(f"Batch {idx + 1} containing {len(batch)} items")
        for item in batch:
            print(
                f" - {item.get('_id', 'N/A')} - {item.get('headline.main', 'No Headline')}"
            )
        # pprint.pprint(batch)

        # print(f"Schema after batch {idx + 1}:")
        # print(source.getSchema())

    source.disconnect()
