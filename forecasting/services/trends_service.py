"""
Google Trends data collection service using pytrends (Free, no API key required).
Fetches search trends for product categories and keywords.
"""

import time
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from django.core.cache import cache
from django.utils import timezone

try:
    from pytrends.request import TrendReq
except ImportError:
    raise ImportError("pytrends library is required. Install with: pip install pytrends")

logger = logging.getLogger(__name__)

# Rate limiting parameters
MIN_DELAY_BETWEEN_REQUESTS = 3  # Seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds

# Default search keywords for Indian market
DEFAULT_KEYWORDS = {
    'toys': ['kids toys', 'baby toys', 'educational toys', 'outdoor games'],
    'clothing': ['baby clothes', 'kids clothing', 'children dress', 'kids wear'],
    'books': ['children books', 'kids books', 'story books'],
    'games': ['board games', 'puzzle games', 'card games'],
    'outdoor': ['outdoor toys', 'bicycles kids', 'sports equipment'],
    'educational': ['learning toys', 'educational games', 'building blocks'],
}


class GoogleTrendsCollector:
    """Collect Google Trends data using pytrends library."""

    def __init__(self, geo: str = "IN", tz: int = 330):
        """
        Initialize Google Trends collector.

        Args:
            geo: Geographic region code (default: IN for India)
            tz: Timezone offset in minutes (default: 330 for IST)
        """
        self.geo = geo
        self.tz = tz
        self.pytrends = TrendReq(hl='en-IN', tz=tz)
        self.last_request_time = 0

    def _apply_rate_limit(self):
        """Apply rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY_BETWEEN_REQUESTS:
            time.sleep(MIN_DELAY_BETWEEN_REQUESTS - elapsed)
        self.last_request_time = time.time()

    def _retry_request(self, func, *args, **kwargs):
        """
        Execute function with retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result or None on failure
        """
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                    time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed after {MAX_RETRIES} attempts: {str(e)}")
                    return None

    def get_interest_over_time(
        self,
        keywords: List[str],
        timeframe: str = 'today 3-m',
        max_keywords: int = 5
    ) -> Optional[Dict]:
        """
        Get interest over time for keywords.

        Args:
            keywords: List of search keywords (max 5 per request)
            timeframe: Time frame ('today 1-m', 'today 3-m', 'today 12-m', 'all')
            max_keywords: Maximum keywords per request (API limit is 5)

        Returns:
            Dictionary with interest data or None on error
        """
        cache_key = f"trends:interest:{':'.join(sorted(keywords[:max_keywords]))}:{timeframe}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        # Limit to max 5 keywords (API limitation)
        keywords = keywords[:max_keywords]

        try:
            self._apply_rate_limit()

            def _fetch():
                self.pytrends.build_payload(
                    keywords,
                    cat=0,
                    timeframe=timeframe,
                    geo=self.geo,
                    gprop=''
                )
                return self.pytrends.interest_over_time()

            df = self._retry_request(_fetch)

            if df is None or df.empty:
                return None

            # Convert to dict for caching
            result = {
                'keywords': keywords,
                'timeframe': timeframe,
                'data': df.to_dict('index'),
                'collected_at': timezone.now().isoformat()
            }

            # Cache for 7 days
            cache.set(cache_key, result, 86400 * 7)
            return result

        except Exception as e:
            logger.error(f"Error getting interest over time: {str(e)}")
            return None

    def get_related_queries(self, keyword: str) -> Optional[Dict]:
        """
        Get related search queries for a keyword.

        Args:
            keyword: Search keyword

        Returns:
            Dictionary with related queries or None on error
        """
        cache_key = f"trends:related:{keyword.lower()}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            self._apply_rate_limit()

            def _fetch():
                self.pytrends.build_payload([keyword], geo=self.geo)
                return self.pytrends.related_queries()

            related = self._retry_request(_fetch)

            if not related:
                return None

            result = {
                'keyword': keyword,
                'related_queries': related,
                'collected_at': timezone.now().isoformat()
            }

            # Cache for 7 days
            cache.set(cache_key, result, 86400 * 7)
            return result

        except Exception as e:
            logger.error(f"Error getting related queries for '{keyword}': {str(e)}")
            return None

    def get_trending_searches(self) -> Optional[List[str]]:
        """
        Get currently trending searches in India.

        Returns:
            List of trending search terms or None on error
        """
        cache_key = "trends:trending:india"
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            self._apply_rate_limit()

            def _fetch():
                return self.pytrends.trending_searches(pn='india')

            trends_df = self._retry_request(_fetch)

            if trends_df is None or trends_df.empty:
                return None

            trending = trends_df[0].tolist()

            # Cache for 1 day (trends change frequently)
            cache.set(cache_key, trending, 86400)
            return trending

        except Exception as e:
            logger.error(f"Error getting trending searches: {str(e)}")
            return None

    def get_category_trends(
        self,
        category_keywords: Dict[str, List[str]],
        timeframe: str = 'today 3-m'
    ) -> Dict[str, Optional[Dict]]:
        """
        Get trends for multiple product categories.

        Args:
            category_keywords: Dictionary mapping categories to keyword lists
            timeframe: Time frame for trends

        Returns:
            Dictionary mapping categories to trend data
        """
        results = {}

        for category, keywords in category_keywords.items():
            logger.info(f"Collecting trends for category: {category}")

            # Collect in batches of 5 (API limit)
            all_data = {}
            for i in range(0, len(keywords), 5):
                batch = keywords[i:i+5]
                trend_data = self.get_interest_over_time(batch, timeframe)

                if trend_data:
                    all_data.update(trend_data.get('data', {}))

            if all_data:
                results[category] = {
                    'category': category,
                    'data': all_data,
                    'collected_at': timezone.now().isoformat()
                }
            else:
                results[category] = None

        return results

    def analyze_keyword_trends(
        self,
        keywords: List[str],
        timeframe: str = 'today 3-m'
    ) -> Optional[Dict]:
        """
        Analyze trends for a set of keywords with additional metrics.

        Args:
            keywords: List of keywords to analyze
            timeframe: Time frame for analysis

        Returns:
            Dictionary with analyzed trend data
        """
        interest_data = self.get_interest_over_time(keywords, timeframe)

        if not interest_data:
            return None

        try:
            import pandas as pd
            from statistics import mean, stdev

            # Convert data to DataFrame
            df = pd.DataFrame(interest_data['data']).T
            df.index = pd.to_datetime(df.index)

            analysis = {
                'keywords': keywords,
                'timeframe': timeframe,
                'trend_statistics': {}
            }

            # Calculate statistics for each keyword
            for keyword in keywords:
                if keyword in df.columns:
                    values = df[keyword].values.tolist()
                    values = [v for v in values if v > 0]  # Filter out zeros

                    if values:
                        analysis['trend_statistics'][keyword] = {
                            'mean': mean(values),
                            'max': max(values),
                            'min': min(values),
                            'std_dev': stdev(values) if len(values) > 1 else 0,
                            'trend': self._calculate_trend(values)
                        }

            analysis['collected_at'] = timezone.now().isoformat()
            return analysis

        except Exception as e:
            logger.error(f"Error analyzing keyword trends: {str(e)}")
            return interest_data  # Return raw data on analysis error

    def _calculate_trend(self, values: List[float]) -> str:
        """
        Calculate trend direction (increasing, decreasing, stable).

        Args:
            values: List of numeric values

        Returns:
            Trend string: 'increasing', 'decreasing', or 'stable'
        """
        if len(values) < 2:
            return 'stable'

        # Compare first half vs second half
        mid = len(values) // 2
        first_half_avg = sum(values[:mid]) / len(values[:mid])
        second_half_avg = sum(values[mid:]) / len(values[mid:])

        change_pct = ((second_half_avg - first_half_avg) / first_half_avg * 100) if first_half_avg > 0 else 0

        if change_pct > 5:
            return 'increasing'
        elif change_pct < -5:
            return 'decreasing'
        else:
            return 'stable'

    def collect_default_categories(self, timeframe: str = 'today 3-m') -> Dict[str, Optional[Dict]]:
        """
        Collect trends for all default product categories.

        Args:
            timeframe: Time frame for trends

        Returns:
            Dictionary mapping categories to trend data
        """
        return self.get_category_trends(DEFAULT_KEYWORDS, timeframe)

    def get_trending_products(self) -> Optional[Dict]:
        """
        Get trending products based on current trending searches.

        Returns:
            Dictionary with trending product analysis
        """
        trending = self.get_trending_searches()

        if not trending:
            return None

        # Filter for product-related trending searches
        product_keywords = [
            t for t in trending
            if any(
                word in t.lower()
                for word in ['kids', 'children', 'baby', 'toys', 'clothes', 'books', 'games']
            )
        ]

        if product_keywords:
            analysis = self.analyze_keyword_trends(product_keywords[:5], 'today 1-m')
            if analysis:
                analysis['trending_searches'] = product_keywords
            return analysis

        return None
