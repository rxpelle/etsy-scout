"""Etsy listing page scraper for competitor analysis.

Scrapes Etsy listing pages to extract price, favorites, reviews,
total sales, shop info, and tags. Handles multiple page layouts
and detects CAPTCHA/bot-block pages.
"""

import re
import json
import logging

import requests
from bs4 import BeautifulSoup

from etsy_scout.http_client import fetch, get_browser_headers
from etsy_scout.rate_limiter import registry as rate_registry
from etsy_scout.config import Config

logger = logging.getLogger(__name__)

LISTING_URL = 'https://www.etsy.com/listing/{listing_id}'


class CaptchaDetected(Exception):
    """Raised when Etsy serves a CAPTCHA or bot-block page."""
    pass


class ListingScraper:
    """Scrapes Etsy listing pages for product metadata."""

    def __init__(self):
        rate_registry.get_limiter('listing_page', rate=Config.LISTING_SCRAPE_RATE_LIMIT)

    def scrape_listing(self, listing_id):
        """Scrape an Etsy listing page for product data.

        Args:
            listing_id: Etsy listing ID (numeric string).

        Returns:
            Dict with listing data fields, or None on failure.

        Raises:
            CaptchaDetected: If Etsy serves a CAPTCHA page.
        """
        rate_registry.acquire('listing_page')

        url = LISTING_URL.format(listing_id=listing_id)
        logger.info(f'Scraping listing page: {url}')

        try:
            response = fetch(url, headers=get_browser_headers())
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.error(f'Network error scraping listing {listing_id}: {e}')
            return None
        except requests.RequestException as e:
            logger.error(f'Request error scraping listing {listing_id}: {e}')
            return None

        if response.status_code == 403:
            raise CaptchaDetected(
                'Etsy returned 403 Forbidden. Try again later or use a proxy.'
            )

        if response.status_code != 200:
            logger.warning(f'Listing page returned {response.status_code} for {listing_id}')
            return None

        html = response.text
        if not html or len(html) < 100:
            logger.warning(f'Empty response for listing {listing_id}')
            return None

        self._check_for_captcha(html)

        soup = BeautifulSoup(html, 'html.parser')

        data = {
            'listing_id': str(listing_id),
            'title': self._parse_title(soup),
            'shop_name': self._parse_shop_name(soup),
            'price': self._parse_price(soup),
            'favorites': self._parse_favorites(soup),
            'review_count': self._parse_review_count(soup),
            'avg_rating': self._parse_avg_rating(soup),
            'total_sales': self._parse_total_sales(soup),
            'tags': self._parse_tags(soup),
            'description': self._parse_description(soup),
            'views': None,  # Not available on listing page
        }

        # Try to get structured data from JSON-LD
        json_ld = self._parse_json_ld(soup)
        if json_ld:
            if data['title'] is None:
                data['title'] = json_ld.get('name')
            if data['price'] is None:
                offers = json_ld.get('offers', {})
                if isinstance(offers, dict):
                    try:
                        data['price'] = float(offers.get('price', 0))
                    except (ValueError, TypeError):
                        pass
            if data['avg_rating'] is None:
                agg = json_ld.get('aggregateRating', {})
                if isinstance(agg, dict):
                    try:
                        data['avg_rating'] = float(agg.get('ratingValue', 0))
                    except (ValueError, TypeError):
                        pass
            if data['review_count'] is None:
                agg = json_ld.get('aggregateRating', {})
                if isinstance(agg, dict):
                    try:
                        data['review_count'] = int(agg.get('reviewCount', 0))
                    except (ValueError, TypeError):
                        pass

        logger.info(
            f'Scraped listing {listing_id}: title="{data["title"]}", '
            f'price={data["price"]}, favorites={data["favorites"]}, '
            f'reviews={data["review_count"]}'
        )

        return data

    def _check_for_captcha(self, html):
        captcha_markers = [
            'Enter the characters you see below',
            'we need to make sure you\'re not a robot',
            'captcha',
            'Please verify you are a human',
            '/captcha/',
        ]
        html_lower = html.lower()
        for marker in captcha_markers:
            if marker.lower() in html_lower:
                raise CaptchaDetected(
                    'Etsy is requesting CAPTCHA verification. '
                    'Try again later or use a proxy.'
                )

    def _parse_title(self, soup):
        # Etsy uses h1 for listing title
        el = soup.select_one('h1[data-buy-box-listing-title]')
        if el:
            return el.get_text(strip=True)

        el = soup.select_one('h1.wt-text-body-01')
        if el:
            return el.get_text(strip=True)

        # Fallback: first h1
        el = soup.select_one('h1')
        if el:
            return el.get_text(strip=True)

        return None

    def _parse_shop_name(self, soup):
        # Shop name link
        el = soup.select_one('a[href*="/shop/"]')
        if el:
            href = el.get('href', '')
            match = re.search(r'/shop/([^/?]+)', href)
            if match:
                return match.group(1)

        # data attribute
        el = soup.find(attrs={'data-shop-name': True})
        if el:
            return el.get('data-shop-name')

        return None

    def _parse_price(self, soup):
        # Price from data attribute
        el = soup.find(attrs={'data-buy-box-listing-price': True})
        if el:
            return self._extract_price(el.get('data-buy-box-listing-price'))

        # Price from visible element
        for selector in [
            'p[data-buy-box-region-price]',
            '.wt-text-title-03',
            'span.currency-value',
        ]:
            el = soup.select_one(selector)
            if el:
                price = self._extract_price(el.get_text())
                if price:
                    return price

        return None

    def _parse_favorites(self, soup):
        # Favorites count
        text = soup.get_text()
        match = re.search(r'([\d,]+)\s+favou?rites?', text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))

        # Data attribute
        el = soup.find(attrs={'data-favorite-count': True})
        if el:
            try:
                return int(el.get('data-favorite-count'))
            except (ValueError, TypeError):
                pass

        return None

    def _parse_review_count(self, soup):
        # Reviews count near the rating
        text = soup.get_text()
        match = re.search(r'([\d,]+)\s+reviews?', text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))

        return None

    def _parse_avg_rating(self, soup):
        # Star rating from aria-label or data
        el = soup.find(attrs={'data-rating': True})
        if el:
            try:
                return float(el.get('data-rating'))
            except (ValueError, TypeError):
                pass

        # Rating from text like "4.8 out of 5 stars"
        text = soup.get_text()
        match = re.search(r'([\d.]+)\s+out\s+of\s+5\s+stars?', text, re.IGNORECASE)
        if match:
            return float(match.group(1))

        return None

    def _parse_total_sales(self, soup):
        # Etsy shows shop sales on listing pages
        text = soup.get_text()
        match = re.search(r'([\d,]+)\s+sales?', text, re.IGNORECASE)
        if match:
            count = int(match.group(1).replace(',', ''))
            if count > 0:
                return count

        return None

    def _parse_tags(self, soup):
        """Extract listing tags. Etsy shows tags in a section."""
        tags = []

        # Tags section
        tag_elements = soup.select('a[href*="search?q="]')
        for el in tag_elements:
            tag_text = el.get_text(strip=True).lower()
            if tag_text and len(tag_text) < 50:
                tags.append(tag_text)

        return list(set(tags))[:20]  # Dedupe, limit to 20

    def _parse_description(self, soup):
        el = soup.select_one('[data-id="description-text"]')
        if el:
            return el.get_text(strip=True)[:500]

        el = soup.select_one('.wt-content-toggle__body')
        if el:
            return el.get_text(strip=True)[:500]

        return None

    def _parse_json_ld(self, soup):
        """Extract structured data from JSON-LD script tags."""
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('@type') == 'Product':
                    return data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'Product':
                            return item
            except (json.JSONDecodeError, TypeError):
                continue
        return None

    def _extract_price(self, text):
        if not text:
            return None
        match = re.search(r'[\$\£\€]?\s*([\d,]+\.?\d*)', str(text))
        if match:
            try:
                price = float(match.group(1).replace(',', ''))
                return price if price > 0 else None
            except ValueError:
                return None
        return None
