"""
Weather data collection service using Open-Meteo API (Free, no API key required).
Fetches current and historical weather data for Indian cities.
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)

# Configuration
OPENMETEO_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Default Indian cities for weather collection
DEFAULT_LOCATIONS = {
    'Mumbai': {'region': 'west'},
    'Delhi': {'region': 'north'},
    'Bangalore': {'region': 'south'},
    'Chennai': {'region': 'south'},
    'Kolkata': {'region': 'east'},
    'Hyderabad': {'region': 'south'},
    'Pune': {'region': 'west'},
    'Ahmedabad': {'region': 'west'},
}

WEATHER_CODES = {
    0: 'Clear sky',
    1: 'Mainly clear',
    2: 'Partly cloudy',
    3: 'Overcast',
    45: 'Foggy',
    48: 'Depositing rime fog',
    51: 'Light drizzle',
    53: 'Moderate drizzle',
    55: 'Dense drizzle',
    61: 'Slight rain',
    63: 'Moderate rain',
    65: 'Heavy rain',
    71: 'Slight snow',
    73: 'Moderate snow',
    75: 'Heavy snow',
    77: 'Snow grains',
    80: 'Slight rain showers',
    81: 'Moderate rain showers',
    82: 'Violent rain showers',
    85: 'Slight snow showers',
    86: 'Heavy snow showers',
    95: 'Thunderstorm',
    96: 'Thunderstorm with slight hail',
    99: 'Thunderstorm with heavy hail',
}


class WeatherDataCollector:
    """Collect weather data from Open-Meteo API (Free)."""

    def __init__(self, timeout: int = 10):
        """
        Initialize weather collector.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()

    def geocode_location(self, location_name: str, country: str = "India") -> Optional[Dict]:
        """
        Get coordinates for a location using Open-Meteo geocoding API.

        Args:
            location_name: City name
            country: Country name (default: India)

        Returns:
            Dictionary with lat, lon, name or None if not found
        """
        cache_key = f"weather:geocode:{location_name.lower()}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            params = {
                'name': location_name,
                'country': country,
                'limit': 1,
                'language': 'en'
            }

            response = self.session.get(
                OPENMETEO_GEOCODING_URL,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            if data.get('results'):
                result = data['results'][0]
                location_info = {
                    'name': result.get('name'),
                    'latitude': result.get('latitude'),
                    'longitude': result.get('longitude'),
                    'country': result.get('country'),
                    'admin1': result.get('admin1'),
                    'timezone': result.get('timezone')
                }
                # Cache for 30 days
                cache.set(cache_key, location_info, 86400 * 30)
                return location_info

        except Exception as e:
            logger.error(f"Error geocoding location {location_name}: {str(e)}")

        return None

    def fetch_current_weather(self, latitude: float, longitude: float) -> Optional[Dict]:
        """
        Fetch current weather data for coordinates.

        Args:
            latitude: Location latitude
            longitude: Location longitude

        Returns:
            Dictionary with weather data or None on error
        """
        try:
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'current': [
                    'temperature_2m',
                    'relative_humidity_2m',
                    'apparent_temperature',
                    'precipitation',
                    'weather_code',
                    'wind_speed_10m',
                    'wind_direction_10m'
                ],
                'timezone': 'Asia/Kolkata'
            }

            response = self.session.get(
                OPENMETEO_FORECAST_URL,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            if data.get('current'):
                return self._parse_weather_data(data['current'])

        except Exception as e:
            logger.error(f"Error fetching current weather for ({latitude}, {longitude}): {str(e)}")

        return None

    def fetch_forecast(self, latitude: float, longitude: float, days: int = 7) -> Optional[List[Dict]]:
        """
        Fetch weather forecast for next N days.

        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of days to forecast (max 16)

        Returns:
            List of daily forecast dictionaries or None on error
        """
        try:
            # Limit to 16 days (API maximum)
            days = min(days, 16)

            params = {
                'latitude': latitude,
                'longitude': longitude,
                'daily': [
                    'temperature_2m_max',
                    'temperature_2m_min',
                    'precipitation_sum',
                    'weather_code',
                    'wind_speed_10m_max'
                ],
                'timezone': 'Asia/Kolkata',
                'forecast_days': days
            }

            response = self.session.get(
                OPENMETEO_FORECAST_URL,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            if data.get('daily'):
                return self._parse_daily_forecast(data['daily'])

        except Exception as e:
            logger.error(f"Error fetching forecast for ({latitude}, {longitude}): {str(e)}")

        return None

    def fetch_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str
    ) -> Optional[List[Dict]]:
        """
        Fetch historical weather data for a date range.

        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)

        Returns:
            List of daily weather dictionaries or None on error
        """
        try:
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'start_date': start_date,
                'end_date': end_date,
                'daily': [
                    'temperature_2m_max',
                    'temperature_2m_min',
                    'precipitation_sum',
                    'weather_code'
                ],
                'timezone': 'Asia/Kolkata'
            }

            response = self.session.get(
                OPENMETEO_ARCHIVE_URL,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            if data.get('daily'):
                return self._parse_daily_forecast(data['daily'])

        except Exception as e:
            logger.error(f"Error fetching historical weather for ({latitude}, {longitude}): {str(e)}")

        return None

    def collect_for_location(self, location_name: str) -> Optional[Dict]:
        """
        Complete weather collection for a single location.

        Args:
            location_name: City name

        Returns:
            Dictionary with all weather data or None on error
        """
        # Geocode location
        location = self.geocode_location(location_name)
        if not location:
            logger.warning(f"Could not geocode location: {location_name}")
            return None

        # Fetch current and forecast
        current = self.fetch_current_weather(location['latitude'], location['longitude'])
        forecast = self.fetch_forecast(location['latitude'], location['longitude'], days=7)

        if current or forecast:
            return {
                'location': location,
                'current': current,
                'forecast': forecast,
                'collected_at': timezone.now().isoformat()
            }

        return None

    def collect_for_default_locations(self) -> Dict[str, Optional[Dict]]:
        """
        Collect weather for all default Indian cities.

        Returns:
            Dictionary mapping city names to weather data
        """
        results = {}

        for location_name in DEFAULT_LOCATIONS.keys():
            logger.info(f"Collecting weather for {location_name}")
            weather_data = self.collect_for_location(location_name)
            results[location_name] = weather_data

            # Rate limiting: small delay between requests
            import time
            time.sleep(1)

        return results

    def _parse_weather_data(self, current: Dict) -> Dict:
        """Parse current weather API response."""
        weather_code = current.get('weather_code', 0)

        return {
            'temperature': current.get('temperature_2m'),
            'apparent_temperature': current.get('apparent_temperature'),
            'humidity': current.get('relative_humidity_2m'),
            'precipitation': current.get('precipitation'),
            'weather_code': weather_code,
            'weather_description': WEATHER_CODES.get(weather_code, 'Unknown'),
            'wind_speed': current.get('wind_speed_10m'),
            'wind_direction': current.get('wind_direction_10m'),
            'timestamp': current.get('time')
        }

    def _parse_daily_forecast(self, daily: Dict) -> List[Dict]:
        """Parse daily forecast API response."""
        forecasts = []
        times = daily.get('time', [])
        temps_max = daily.get('temperature_2m_max', [])
        temps_min = daily.get('temperature_2m_min', [])
        precip = daily.get('precipitation_sum', [])
        weather_codes = daily.get('weather_code', [])
        wind_speeds = daily.get('wind_speed_10m_max', [])

        for i in range(len(times)):
            weather_code = weather_codes[i] if i < len(weather_codes) else 0

            forecast_item = {
                'date': times[i],
                'temperature_max': temps_max[i] if i < len(temps_max) else None,
                'temperature_min': temps_min[i] if i < len(temps_min) else None,
                'precipitation': precip[i] if i < len(precip) else None,
                'weather_code': weather_code,
                'weather_description': WEATHER_CODES.get(weather_code, 'Unknown'),
                'wind_speed_max': wind_speeds[i] if i < len(wind_speeds) else None,
                'temperature_avg': (
                    (temps_max[i] + temps_min[i]) / 2
                    if i < len(temps_max) and i < len(temps_min)
                    else None
                )
            }
            forecasts.append(forecast_item)

        return forecasts

    def close(self):
        """Close HTTP session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
