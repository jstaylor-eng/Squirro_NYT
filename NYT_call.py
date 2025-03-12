import argparse
import logging
import requests
import time
import json
import os
import pprint
from typing import Generator, List, Dict, Any

key = "kAz72eY5anyqXf8pAdWLblK1xWhKZg4k"

NYT_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json?"


log = logging.getLogger(__name__)


class NYTimesSource:
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self, api_key, query):
        self.api_key = api_key
        self.query = query
        self.schema = set()

    def connect(self, inc_column=None, max_inc_value=None):
        """Connect to the source"""
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        pass

    def _get_data(self, page=0):
        extension = {"q": self.query, "api-key": self.api_key, "page": page}
        # print(NYT_URL + extension)
        response = requests.get(NYT_URL, params=extension)
        if response.status_code == 200:
            # print(response.status_code)
            # print(response.text)
            return response.json()
        else:
            log.error("Error fetching data: %s", response.text)
            return None

    def _flatten_dict(self, d, parent_key="", sep="."):
        items = []
        # loop through keys and values
        for k, v in d.items():
            # if nested, create new key with "." between k and v
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):  # if value is dict, flatten (recursive)
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:  # else keep key and value as is
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
