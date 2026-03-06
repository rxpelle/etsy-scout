"""Shared HTTP client with retry logic, user-agent rotation, and proxy support."""

import random
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

from etsy_scout.config import Config

logger = logging.getLogger(__name__)


def create_session(proxy_url=None):
    """Create a configured requests.Session with retry logic."""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET', 'POST'],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    if proxy_url or Config.PROXY_URL:
        url = proxy_url or Config.PROXY_URL
        session.proxies = {
            'http': url,
            'https': url,
        }
        logger.info(f'HTTP client using proxy: {url[:30]}...')

    session.timeout = 15
    return session


def get_random_user_agent():
    """Return a random user agent string from the configured list."""
    return random.choice(Config.USER_AGENTS)


def get_headers():
    """Return request headers with a rotated user agent."""
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
    }


def get_browser_headers():
    """Return request headers that closely mimic a real browser."""
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }


_session = None


def get_session():
    """Get the shared HTTP session, creating it if needed."""
    global _session
    if _session is None:
        _session = create_session()
    return _session


def fetch(url, params=None, headers=None):
    """Make a GET request with retry logic and user-agent rotation."""
    session = get_session()
    request_headers = get_headers()
    if headers:
        request_headers.update(headers)

    logger.debug(f'GET {url} params={params}')

    response = session.get(url, params=params, headers=request_headers, timeout=15)

    logger.debug(f'Response: {response.status_code} ({len(response.content)} bytes)')

    if response.status_code == 429:
        logger.warning(f'Rate limited (429) on {url}')
    elif response.status_code >= 400:
        logger.warning(f'HTTP {response.status_code} on {url}')

    return response
