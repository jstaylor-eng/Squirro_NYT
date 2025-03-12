import argparse
import logging
import requests
import time
import json
import os
import pprint
from typing import Generator, List, Dict, Any

log = logging.getLogger(__name__)

NYT_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json?"


class NYTimesSource:
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self, api_key, query):
        self.api_key = api_key
        self.query = query
        self.inc_column = inc_column  # column for incremental updates
        self.max_inc_value = max_inc_value  # Last known value
        self.max_retries = max_retries
        self.schema = set()

    def connect(self, inc_column=None, max_inc_value=None):
        """Connect to the source"""
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        pass

    def _get_data(self, page=0):
        retries = 0
        max_retries = 5
        params = {"q": self.query, "api-key": self.api_key, "page": page}
        # print(NYT_URL, params) # check for working url
        response = requests.get(NYT_URL, params=params)
        if response.status_code == 200:
            # pprint.pprint(response.text) # raw response check (import pprint)
            return response.json()
        elif retries < max_retries:
            retries += 1
            time.sleep(10)
            pass
        else:
            log.error("Error fetching data: %s", response.text)
            return None

    def _flatten_dict(self, x, parent_key="", sep="."):
        items = []
        # loop through keys and values
        for k, v in x.items():
            # if nested dict, create new key with "." between k and v
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                # if value is dict, flatten (recursivly) and append to items.
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:  # else keep key and value as is and append to items
                items.append((new_key, v))
        # pprint.pprint(dict(items))
        return dict(items)

    def getDataBatch(self, batch_size=5):
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

            batch = [self._flatten_dict(article) for article in articles]
            self.schema.update(key for article in batch for key in article.keys())
            yield batch

            page += 1
            time.sleep(1)
            if len(batch) > batch_size:
                break

            # TODO: implement - this dummy implementation returns one batch of data
            # yield [
            #     {
            #         "headline.main": "The main headline",
            #         "_id": "1234",
            #     }
            # ]

    def getSchema(self) -> List[str]:
        print(sorted(self.schema))
        return sorted(self.schema)


if __name__ == "__main__":
    config = {
        "api_key": os.getenv("API_KEY"),
        "query": "Silicon Valley",
    }

    source = NYTimesSource(**config)
    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    # source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print(f"{idx + 1} Batch of {len(batch)} items")
        for item in batch:
            print(
                f" - {item.get('_id', 'N/A')} - {item.get('headline.main', 'No Headline')}"
            )

    source.disconnect()
