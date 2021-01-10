from aiohttp import ClientSession, ClientHttpProxyError
import asyncio
import datetime
import requests
import json
from call_settings import jira_server, username, password, project, period
import time
import pandas as pd
import logging
from datetime import date

today = date.today()

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def post_jira_api_request(url, user, pswd):
    auth = (user, pswd)
    headers = {
        "Accept": "application/json"
    }

    response = requests.request(
        "GET",
        url,
        headers=headers,
        auth=auth
    )
    return response


# an initial request is posted to the api to grab the {project} issues that have been updated in the last {x} period.
# the issues appended to the list can then be separately queried to bring back their full data.
# cookies are also saved at this point to prevent having to establish a new connection with credentials each time.
# issues limited to 1000 per request so code refresh schedule should be frequent enough to prevent this being an issue.
issues_to_request = []
issues_updated = post_jira_api_request(
    f"{jira_server}/rest/api/2/search?jql=project in ({project}) and updated>=-{period}"
    f"&maxResults=1000&fields=*none",
    username, password)
cookies = issues_updated.cookies.get_dict()

for issue in issues_updated.json()["issues"]:
    issues_to_request.append(issue["key"])

logger.info(issues_to_request)

START = time.monotonic()
api_output = []


class RateLimiter:
    """Rate limits an HTTP client that would make get() and post() calls.
    Calls are rate-limited by host.
    Prevents overloading of server and ensuing timeout errors."""
    RATE = 1  # one request per second
    MAX_TOKENS = 10

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        now = time.monotonic() - START
        logger.info(f'{now:.0f}s: req {args[0]}')
        return self.client.get(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens < 1:
            self.add_new_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


async def clean_response(response, api_req):
    logger.info(f"cleaning {api_req}")
    response["api_extract_date"] = str(datetime.datetime.now())
    return response


async def acquire_one(client, i):
    try:
        url = f"{jira_server}/rest/api/2/issue/{i}/?expand=names&fields=-project,-customfield_12500"
        async with await client.get(url, cookies=cookies, headers={"Accept": "application/json"}) as resp:
            resp = await resp.json()
            now = time.monotonic() - START
            logger.info(f"{now:.0f}s: got {i}")
            resp = await clean_response(resp, i)
            api_output.append({"process": i, "date": datetime.datetime.now(), "payload": json.dumps(resp)})
    except ClientHttpProxyError as http_err:
        logger.error(f"failed http {i}")
        logger.error(f"HTTP error occurred: {http_err}")
    except asyncio.TimeoutError:
        logger.error({"results": f"timeout error on {i}"})
    except Exception as err:
        logger.error(f"failed {i}")
        logger.error(f"An error ocurred: {Exception, err}")


async def main():
    async with ClientSession() as session:
        session = RateLimiter(session)
        tasks = [asyncio.ensure_future(acquire_one(session, i)) for i in issues_to_request]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
    if len(api_output) > 0:
        jira_api_output = pd.DataFrame(api_output)
        print(jira_api_output)
        logger.info("dataframe printed")
        jira_api_output.to_csv(f'outputs\\{project}_jira_updates_{today.strftime("%d%m%Y")}.csv', index=False)
    else:
        logger.info("Nothing to download")
