"""
Utilities for loading and caching external data sources.
Handles festival calendars, seasonal patterns, and static data files.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from django.core.cache import cache
from django.utils import timezone


class DataLoaderException(Exception):
    """Custom exception for data loading errors."""
    pass


def get_static_data_path(filename: str) -> Optional[Path]:
    """
    Get absolute path to a static data JSON file.
    Returns None if file doesn't exist instead of raising exception.
    """
    base_dir = Path(__file__).resolve().parent.parent / "static_data"
    filepath = base_dir / filename

    if not filepath.exists():
        return None

    return filepath


def load_json_file(filename: str, default: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Load and parse a JSON static data file.
    Returns default dict if file missing or invalid, never raises exceptions.

    Args:
        filename: JSON filename to load
        default: Default dict to return if file cannot be loaded

    Returns:
        Loaded data or default dict (guaranteed to return a dict)
    """
    if default is None:
        default = {}

    try:
        filepath = get_static_data_path(filename)
        if not filepath:
            return default

        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if data else default
    except json.JSONDecodeError:
        return default
    except Exception:
        return default


def load_festival_calendar(use_cache: bool = True) -> Dict[str, Any]:
    """
    Load and cache Indian festival calendar.
    Returns safe default if file missing, never raises exceptions.

    Args:
        use_cache: Whether to use cached data (default: True)

    Returns:
        Dictionary with festival data (guaranteed valid)
    """
    cache_key = "forecasting:festivals:calendar"

    if use_cache:
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data

    # Default structure if file missing
    default_festivals = {
        'festivals': [
            {
                'name': 'Diwali',
                'type': 'major',
                'region': 'all',
                'demand_multiplier': 1.8,
                'impact_window_days': 14,
                'dates': {},
                'impact_categories': ['toys', 'clothing', 'gifts']
            }
        ]
    }

    festivals_data = load_json_file("indian_festivals.json", default=default_festivals)

    # Cache for 24 hours
    if use_cache:
        cache.set(cache_key, festivals_data, 86400)

    return festivals_data


def load_seasonal_patterns(use_cache: bool = True) -> Dict[str, Any]:
    """
    Load and cache seasonal patterns and multipliers.
    Returns safe default if file missing, never raises exceptions.

    Args:
        use_cache: Whether to use cached data (default: True)

    Returns:
        Dictionary with seasonal data (guaranteed valid)
    """
    cache_key = "forecasting:seasonal:patterns"

    if use_cache:
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data

    # Default structure if file missing
    default_patterns = {
        'seasons': {
            'winter': {'months': [11, 12, 1, 2], 'category_multipliers': {}},
            'summer': {'months': [3, 4, 5, 6], 'category_multipliers': {}},
            'monsoon': {'months': [7, 8, 9], 'category_multipliers': {}},
            'spring': {'months': [10], 'category_multipliers': {}}
        },
        'day_of_week_patterns': {
            'monday': 1.0, 'tuesday': 1.0, 'wednesday': 1.0,
            'thursday': 1.0, 'friday': 1.1, 'saturday': 1.2, 'sunday': 1.1
        },
        'month_patterns': {str(i): 1.0 for i in range(1, 13)},
        'weather_impact': {},
        'temperature_impact': {},
        'product_lifecycle_phases': {
            'launch': {'days_from_created': '0-30', 'demand_multiplier': 0.8},
            'growth': {'days_from_created': '31-180', 'demand_multiplier': 1.3},
            'mature': {'days_from_created': '181-730', 'demand_multiplier': 1.0},
            'decline': {'days_from_created': '731-999999', 'demand_multiplier': 0.7}
        }
    }

    seasonal_data = load_json_file("seasonal_patterns.json", default=default_patterns)

    # Cache for 24 hours
    if use_cache:
        cache.set(cache_key, seasonal_data, 86400)

    return seasonal_data


def get_festivals_in_range(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """
    Get all festivals within a date range.
    Never fails - returns empty list if data unavailable.

    Args:
        start_date: Range start date
        end_date: Range end date

    Returns:
        List of festival dictionaries (guaranteed)
    """
    try:
        festivals_data = load_festival_calendar()
        matching_festivals = []

        current_year = start_date.year
        end_year = end_date.year

        for festival in festivals_data.get('festivals', []):
            festival_dates = festival.get('dates', {})

            for year in range(current_year, end_year + 1):
                year_str = str(year)
                if year_str in festival_dates:
                    try:
                        festival_date_str = festival_dates[year_str]
                        festival_date = datetime.strptime(festival_date_str, '%Y-%m-%d').date()

                        if start_date.date() <= festival_date <= end_date.date():
                            festival_info = festival.copy()
                            festival_info['festival_date'] = festival_date.isoformat()
                            festival_info['days_until'] = (festival_date - start_date.date()).days
                            matching_festivals.append(festival_info)
                    except:
                        continue

        # Sort by date
        matching_festivals.sort(key=lambda x: x.get('festival_date', ''))

        return matching_festivals
    except:
        return []


def get_upcoming_festivals(days_ahead: int = 30) -> List[Dict[str, Any]]:
    """
    Get upcoming festivals within next N days.

    Args:
        days_ahead: Number of days to look ahead (default: 30)

    Returns:
        List of upcoming festivals
    """
    today = timezone.now()
    end_date = today + timedelta(days=days_ahead)

    return get_festivals_in_range(today, end_date)


def get_seasonal_multiplier(category: str, month: int) -> float:
    """
    Get seasonal demand multiplier for a category in a given month.

    Args:
        category: Product category name
        month: Month number (1-12)

    Returns:
        Demand multiplier (1.0 = baseline)
    """
    seasonal_data = load_seasonal_patterns()

    # Determine season from month
    season = None
    for season_name, season_info in seasonal_data.get('seasons', {}).items():
        if month in season_info.get('months', []):
            season = season_name
            break

    if not season:
        return 1.0

    season_info = seasonal_data['seasons'][season]
    category_multipliers = season_info.get('category_multipliers', {})

    # Return category multiplier or default to 1.0
    return category_multipliers.get(category, 1.0)


def get_festival_impact(festival_name: str) -> Optional[Dict[str, Any]]:
    """
    Get impact information for a specific festival.

    Args:
        festival_name: Name of the festival

    Returns:
        Festival impact dictionary or None if not found
    """
    festivals_data = load_festival_calendar()

    for festival in festivals_data.get('festivals', []):
        if festival.get('name', '').lower() == festival_name.lower():
            return {
                'name': festival.get('name'),
                'demand_multiplier': festival.get('demand_multiplier', 1.0),
                'impact_window_days': festival.get('impact_window_days', 7),
                'impact_categories': festival.get('impact_categories', []),
                'type': festival.get('type'),
                'region': festival.get('region')
            }

    return None


def get_day_of_week_multiplier(weekday: int) -> float:
    """
    Get demand multiplier for day of week.

    Args:
        weekday: Day of week (0=Monday, 6=Sunday)

    Returns:
        Demand multiplier
    """
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    if weekday < 0 or weekday > 6:
        return 1.0

    seasonal_data = load_seasonal_patterns()
    day_patterns = seasonal_data.get('day_of_week_patterns', {})

    return day_patterns.get(days[weekday], 1.0)


def get_month_multiplier(month: int) -> float:
    """
    Get seasonal demand multiplier for a given month.

    Args:
        month: Month number (1-12)

    Returns:
        Demand multiplier
    """
    if month < 1 or month > 12:
        return 1.0

    seasonal_data = load_seasonal_patterns()
    month_patterns = seasonal_data.get('month_patterns', {})

    return month_patterns.get(str(month), 1.0)


def get_weather_impact(weather_code: str) -> float:
    """
    Get demand impact factor for weather conditions.

    Args:
        weather_code: OpenWeatherMap weather code description

    Returns:
        Impact multiplier
    """
    seasonal_data = load_seasonal_patterns()
    weather_impact = seasonal_data.get('weather_impact', {})

    # Normalize weather code
    weather_code_normalized = weather_code.lower().replace('_', ' ')

    for key, multiplier in weather_impact.items():
        if key.lower() in weather_code_normalized:
            return multiplier

    return 1.0


def get_temperature_impact(temperature_celsius: float) -> float:
    """
    Get demand impact factor for temperature.

    Args:
        temperature_celsius: Temperature in Celsius

    Returns:
        Impact multiplier
    """
    seasonal_data = load_seasonal_patterns()
    temp_impact = seasonal_data.get('temperature_impact', {})

    if temperature_celsius < 10:
        return temp_impact.get('below_10', 1.0)
    elif temperature_celsius < 15:
        return temp_impact.get('10_to_15', 1.0)
    elif temperature_celsius < 20:
        return temp_impact.get('15_to_20', 1.0)
    elif temperature_celsius < 25:
        return temp_impact.get('20_to_25', 1.0)
    elif temperature_celsius < 30:
        return temp_impact.get('25_to_30', 1.0)
    elif temperature_celsius < 35:
        return temp_impact.get('30_to_35', 1.0)
    elif temperature_celsius < 40:
        return temp_impact.get('35_to_40', 1.0)
    else:
        return temp_impact.get('above_40', 1.0)


def get_product_lifecycle_multiplier(created_date: datetime) -> float:
    """
    Get demand multiplier based on product lifecycle phase.

    Args:
        created_date: Product creation date

    Returns:
        Lifecycle multiplier
    """
    days_since_launch = (timezone.now() - created_date).days
    seasonal_data = load_seasonal_patterns()
    lifecycle_phases = seasonal_data.get('product_lifecycle_phases', {})

    for phase_name, phase_info in lifecycle_phases.items():
        days_range = phase_info.get('days_from_created', '0-30')
        start, end = map(int, days_range.split('-'))

        if end == 730:  # 'mature' phase
            if days_since_launch >= start:
                return phase_info.get('demand_multiplier', 1.0)
        else:
            if start <= days_since_launch <= end:
                return phase_info.get('demand_multiplier', 1.0)

    return 1.0


def clear_static_data_cache():
    """Clear all cached static data."""
    cache.delete("forecasting:festivals:calendar")
    cache.delete("forecasting:seasonal:patterns")
    return True


def get_all_static_data_info() -> Dict[str, Any]:
    """
    Get information about all available static data.

    Returns:
        Dictionary with data availability info
    """
    return {
        'festivals': {
            'loaded': True,
            'festivals_count': len(load_festival_calendar().get('festivals', [])),
            'cache_ttl': 86400
        },
        'seasonal_patterns': {
            'loaded': True,
            'seasons_count': len(load_seasonal_patterns().get('seasons', {})),
            'cache_ttl': 86400
        }
    }
