# NYTimes Article Search tool

This Python project uses the New York Times Aricle Search API, returning batches of articles in response to a search query, in a flattened dictionary format.

---

## Get started:

1. Go to the NYT developer site (https://developer.nytimes.com/) and register for an account. Once your account is activated, go to Apps (https://developer.nytimes.com/my-apps) and Create an API key with "Article Search API" **Enbled**. Copy your API key.

2. Clone this directory and complete the following steps to set up: 

```bash
git clone https://github.com/jstaylor-eng/Squirro_NYT

# Make a virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install requirements.txt
pip install -r requirements.txt

```

3. In the root directory (where nyt_call.py is located) create a file called ".env" and open it. Add the following, where "my_api_key" is replaced with your API key copied from step 1:
API_KEY=my_api_key

4. Change the value of "query" in in nyt_call.py if required, and run using:
```bash
python3 nyt_call.py
