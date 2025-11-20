"""
Feature engineering service for forecasting models.
Creates features from external data sources and product history.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from django.utils import timezone
from django.db.models import Avg, Sum, Count, Q
from django.core.cache import cache

from forecasting.utils.data_loaders import (
    get_upcoming_festivals,
    get_seasonal_multiplier,
    get_day_of_week_multiplier,
    get_month_multiplier,
    get_festival_impact,
    get_product_lifecycle_multiplier,
    get_temperature_impact,
    get_weather_impact
)

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Create features for forecasting models from multiple data sources."""

    @staticmethod
    def create_features_for_sku(
        sku_code: str,
        forecast_date: datetime,
        include_external: bool = True,
        include_weather: bool = True,
        include_trends: bool = True
    ) -> Dict[str, Any]:
        """
        Create comprehensive feature set for SKU forecasting.
        Never returns None - returns safe defaults if any step fails.

        Args:
            sku_code: Product SKU code
            forecast_date: Date for which to create features
            include_external: Include external data features
            include_weather: Include weather features
            include_trends: Include Google Trends features

        Returns:
            Dictionary of features (guaranteed to have safe defaults)
        """
        try:
            features = {}

            # Basic temporal features
            features.update(FeatureEngineer._create_temporal_features(forecast_date))

            # Seasonal features
            features.update(FeatureEngineer._create_seasonal_features(sku_code, forecast_date))

            # Festival features
            if include_external:
                features.update(FeatureEngineer._create_festival_features(sku_code, forecast_date))

            # Product lifecycle features
            features.update(FeatureEngineer._create_product_lifecycle_features(sku_code))

            # Historical sales patterns
            features.update(FeatureEngineer._create_historical_features(sku_code, forecast_date))

            # Weather features
            if include_weather:
                features.update(FeatureEngineer._create_weather_features(forecast_date))

            # Trends features
            if include_trends:
                features.update(FeatureEngineer._create_trends_features(sku_code, forecast_date))

            return features

        except Exception as e:
            logger.error(f"Error creating features for {sku_code}: {str(e)}")
            # Return safe default features
            return FeatureEngineer._get_default_features(forecast_date)

    @staticmethod
    def _get_default_features(forecast_date: datetime) -> Dict[str, Any]:
        """Get default safe features when feature engineering fails."""
        return {
            # Temporal
            'day_of_week': forecast_date.weekday(),
            'day_of_month': forecast_date.day,
            'month': forecast_date.month,
            'quarter': (forecast_date.month - 1) // 3 + 1,
            'week_of_year': forecast_date.isocalendar()[1],
            'is_weekend': 1 if forecast_date.weekday() >= 5 else 0,
            'is_month_end': 1 if forecast_date.day >= 25 else 0,
            'is_month_start': 1 if forecast_date.day <= 5 else 0,
            'is_quarter_end': 1 if forecast_date.day >= 25 and forecast_date.month % 3 == 0 else 0,
            'day_of_week_multiplier': 1.0,
            'month_multiplier': 1.0,
            # Seasonal
            'seasonal_multiplier': 1.0,
            'season': 'unknown',
            'category_season_mult': 1.0,
            # Festival
            'is_festival_week': 0,
            'festival_name': None,
            'festival_multiplier': 1.0,
            'days_to_festival': 999,
            'festival_impact_window': 0,
            # Lifecycle
            'days_since_launch': 999,
            'lifecycle_stage': 'unknown',
            'lifecycle_multiplier': 1.0,
            # Historical
            'avg_daily_sales_7d': 0,
            'avg_daily_sales_30d': 0,
            'avg_daily_sales_90d': 0,
            'sales_trend_7d': 'stable',
            'sales_volatility_7d': 0,
            'days_since_last_sale': 999,
            'stockout_occurred': 0,
            # Weather
            'temperature': None,
            'humidity': None,
            'precipitation': None,
            'weather_code': None,
            'temperature_impact': 1.0,
            'weather_impact': 1.0,
            'has_weather_data': False,
            # Trends
            'trend_score': 50,
            'trend_direction': 'stable',
            'has_trend_data': False
        }

    @staticmethod
    def _create_temporal_features(forecast_date: datetime) -> Dict[str, Any]:
        """Create temporal/calendar features."""
        return {
            'day_of_week': forecast_date.weekday(),
            'day_of_month': forecast_date.day,
            'month': forecast_date.month,
            'quarter': (forecast_date.month - 1) // 3 + 1,
            'week_of_year': forecast_date.isocalendar()[1],
            'is_weekend': 1 if forecast_date.weekday() >= 5 else 0,
            'is_month_end': 1 if forecast_date.day >= 25 else 0,
            'is_month_start': 1 if forecast_date.day <= 5 else 0,
            'is_quarter_end': 1 if forecast_date.day >= 25 and forecast_date.month % 3 == 0 else 0,
            'day_of_week_multiplier': get_day_of_week_multiplier(forecast_date.weekday()),
            'month_multiplier': get_month_multiplier(forecast_date.month)
        }

    @staticmethod
    def _create_seasonal_features(sku_code: str, forecast_date: datetime) -> Dict[str, Any]:
        """Create seasonal features."""
        try:
            from variant.models import ProductVariant
            from category.models import Category

            variant = ProductVariant.objects.filter(sku=sku_code).first()
            if not variant:
                return {'seasonal_multiplier': 1.0, 'season': 'unknown'}

            # Get category seasonal multiplier
            category = variant.product.category if hasattr(variant.product, 'category') else None
            category_name = category.name if category else 'default'

            seasonal_multiplier = get_seasonal_multiplier(
                category_name.lower(),
                forecast_date.month
            )

            # Determine season
            month = forecast_date.month
            if month in [11, 12, 1, 2]:
                season = 'winter'
            elif month in [3, 4, 5, 6]:
                season = 'summer'
            elif month in [7, 8, 9]:
                season = 'monsoon'
            else:
                season = 'spring'

            return {
                'seasonal_multiplier': seasonal_multiplier,
                'season': season,
                'category_season_mult': seasonal_multiplier
            }

        except Exception as e:
            logger.warning(f"Error creating seasonal features for {sku_code}: {str(e)}")
            return {'seasonal_multiplier': 1.0, 'season': 'unknown'}

    @staticmethod
    def _create_festival_features(sku_code: str, forecast_date: datetime) -> Dict[str, Any]:
        """Create festival-related features."""
        try:
            features = {
                'is_festival_week': 0,
                'festival_name': None,
                'festival_multiplier': 1.0,
                'days_to_festival': 999,
                'festival_impact_window': 0
            }

            # Get upcoming festivals
            upcoming_festivals = get_upcoming_festivals(days_ahead=60)

            # Check if forecast_date is near any festival
            for festival in upcoming_festivals:
                festival_date = datetime.strptime(
                    festival['festival_date'],
                    '%Y-%m-%d'
                ).date()
                days_diff = (festival_date - forecast_date.date()).days

                # Check if within impact window
                impact_window = festival.get('impact_window_days', 7)
                if -impact_window <= days_diff <= impact_window:
                    features['is_festival_week'] = 1
                    features['festival_name'] = festival['name']
                    features['festival_multiplier'] = festival.get('demand_multiplier', 1.0)
                    features['days_to_festival'] = days_diff
                    features['festival_impact_window'] = impact_window
                    break
                elif 0 < days_diff < features['days_to_festival']:
                    features['days_to_festival'] = days_diff

            return features

        except Exception as e:
            logger.warning(f"Error creating festival features: {str(e)}")
            return {
                'is_festival_week': 0,
                'festival_name': None,
                'festival_multiplier': 1.0,
                'days_to_festival': 999,
                'festival_impact_window': 0
            }

    @staticmethod
    def _create_product_lifecycle_features(sku_code: str) -> Dict[str, Any]:
        """Create product lifecycle features based on created_at."""
        try:
            from variant.models import ProductVariant

            variant = ProductVariant.objects.filter(sku=sku_code).first()
            if not variant or not variant.created_at:
                return {
                    'days_since_launch': 999,
                    'lifecycle_stage': 'unknown',
                    'lifecycle_multiplier': 1.0
                }

            days_since_launch = (timezone.now() - variant.created_at).days

            if days_since_launch < 30:
                stage = 'launch'
            elif days_since_launch < 180:
                stage = 'growth'
            elif days_since_launch < 730:
                stage = 'maturity'
            else:
                stage = 'decline'

            lifecycle_multiplier = get_product_lifecycle_multiplier(variant.created_at)

            return {
                'days_since_launch': days_since_launch,
                'lifecycle_stage': stage,
                'lifecycle_multiplier': lifecycle_multiplier
            }

        except Exception as e:
            logger.warning(f"Error creating product lifecycle features for {sku_code}: {str(e)}")
            return {
                'days_since_launch': 999,
                'lifecycle_stage': 'unknown',
                'lifecycle_multiplier': 1.0
            }

    @staticmethod
    def _create_historical_features(sku_code: str, forecast_date: datetime) -> Dict[str, Any]:
        """Create features from historical sales data."""
        try:
            from forecasting.models import HistoricalSalesDaily

            features = {
                'avg_daily_sales_7d': 0,
                'avg_daily_sales_30d': 0,
                'avg_daily_sales_90d': 0,
                'sales_trend_7d': 'stable',
                'sales_volatility_7d': 0,
                'days_since_last_sale': 999,
                'stockout_occurred': 0
            }

            # Historical sales data
            end_date = forecast_date - timedelta(days=1)
            start_7d = end_date - timedelta(days=6)
            start_30d = end_date - timedelta(days=29)
            start_90d = end_date - timedelta(days=89)

            sales_7d = HistoricalSalesDaily.objects.filter(
                sku_code=sku_code,
                sale_date__range=[start_7d, end_date]
            )
            sales_30d = HistoricalSalesDaily.objects.filter(
                sku_code=sku_code,
                sale_date__range=[start_30d, end_date]
            )
            sales_90d = HistoricalSalesDaily.objects.filter(
                sku_code=sku_code,
                sale_date__range=[start_90d, end_date]
            )

            # Calculate averages
            if sales_7d.exists():
                avg_7d = sales_7d.aggregate(Avg('quantity_sold'))['quantity_sold__avg'] or 0
                features['avg_daily_sales_7d'] = avg_7d

            if sales_30d.exists():
                avg_30d = sales_30d.aggregate(Avg('quantity_sold'))['quantity_sold__avg'] or 0
                features['avg_daily_sales_30d'] = avg_30d

            if sales_90d.exists():
                avg_90d = sales_90d.aggregate(Avg('quantity_sold'))['quantity_sold__avg'] or 0
                features['avg_daily_sales_90d'] = avg_90d

            # Trend detection
            if sales_7d.count() >= 2:
                values = list(sales_7d.order_by('sale_date').values_list('quantity_sold', flat=True))
                features['sales_trend_7d'] = FeatureEngineer._calculate_trend(values)

                # Volatility
                if len(values) > 1:
                    import statistics
                    try:
                        features['sales_volatility_7d'] = statistics.stdev(values)
                    except:
                        features['sales_volatility_7d'] = 0

            # Days since last sale
            last_sale = sales_7d.order_by('-sale_date').first()
            if last_sale:
                days_diff = (end_date.date() - last_sale.sale_date).days
                features['days_since_last_sale'] = max(days_diff, 0)

            return features

        except Exception as e:
            logger.warning(f"Error creating historical features for {sku_code}: {str(e)}")
            return {
                'avg_daily_sales_7d': 0,
                'avg_daily_sales_30d': 0,
                'avg_daily_sales_90d': 0,
                'sales_trend_7d': 'stable',
                'sales_volatility_7d': 0,
                'days_since_last_sale': 999,
                'stockout_occurred': 0
            }

    @staticmethod
    def _create_weather_features(forecast_date: datetime) -> Dict[str, Any]:
        """Create weather-related features."""
        try:
            from forecasting.models import ExternalDataSource

            features = {
                'temperature': None,
                'humidity': None,
                'precipitation': None,
                'weather_code': None,
                'temperature_impact': 1.0,
                'weather_impact': 1.0,
                'has_weather_data': False
            }

            # Get weather data for forecast date
            weather_data = ExternalDataSource.objects.filter(
                data_type='weather',
                data_date=forecast_date.date()
            ).first()

            if weather_data and weather_data.raw_data:
                raw = weather_data.raw_data
                if isinstance(raw, dict):
                    features['temperature'] = raw.get('temperature')
                    features['humidity'] = raw.get('humidity')
                    features['precipitation'] = raw.get('precipitation')
                    features['weather_code'] = raw.get('weather_code')
                    features['has_weather_data'] = True

                    # Calculate impacts
                    if features['temperature']:
                        features['temperature_impact'] = get_temperature_impact(
                            features['temperature']
                        )

                    if features['weather_code']:
                        features['weather_impact'] = get_weather_impact(
                            str(features['weather_code'])
                        )

            return features

        except Exception as e:
            logger.warning(f"Error creating weather features: {str(e)}")
            return {
                'temperature': None,
                'humidity': None,
                'precipitation': None,
                'weather_code': None,
                'temperature_impact': 1.0,
                'weather_impact': 1.0,
                'has_weather_data': False
            }

    @staticmethod
    def _create_trends_features(sku_code: str, forecast_date: datetime) -> Dict[str, Any]:
        """Create Google Trends features."""
        try:
            from forecasting.models import ExternalDataSource
            from variant.models import ProductVariant

            features = {
                'trend_score': 50,
                'trend_direction': 'stable',
                'has_trend_data': False
            }

            # Get product category
            variant = ProductVariant.objects.filter(sku=sku_code).first()
            if not variant:
                return features

            # Look for trends data for this category
            category_name = None
            try:
                if hasattr(variant, 'product') and hasattr(variant.product, 'category'):
                    category_name = variant.product.category.name
            except:
                pass

            if category_name:
                trends_data = ExternalDataSource.objects.filter(
                    data_type='trends',
                    raw_data__contains=category_name,
                    data_date__lte=forecast_date.date()
                ).order_by('-data_date').first()

                if trends_data and trends_data.raw_data:
                    raw = trends_data.raw_data
                    if isinstance(raw, dict):
                        features['trend_score'] = raw.get('score', 50)
                        features['trend_direction'] = raw.get('direction', 'stable')
                        features['has_trend_data'] = True

            return features

        except Exception as e:
            logger.warning(f"Error creating trends features: {str(e)}")
            return {
                'trend_score': 50,
                'trend_direction': 'stable',
                'has_trend_data': False
            }

    @staticmethod
    def _calculate_trend(values: List[float]) -> str:
        """Calculate trend direction."""
        if len(values) < 2:
            return 'stable'

        mid = len(values) // 2
        if mid == 0:
            return 'stable'

        first_half = sum(values[:mid]) / len(values[:mid])
        second_half = sum(values[mid:]) / len(values[mid:])

        if first_half == 0:
            return 'stable'

        change_pct = ((second_half - first_half) / first_half) * 100

        if change_pct > 10:
            return 'increasing'
        elif change_pct < -10:
            return 'decreasing'
        else:
            return 'stable'

    @staticmethod
    def create_bulk_features(
        sku_codes: List[str],
        forecast_date: datetime,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """
        Create features for multiple SKUs.
        Never returns None values - always returns valid features for each SKU.

        Args:
            sku_codes: List of SKU codes
            forecast_date: Forecast date
            **kwargs: Additional arguments for create_features_for_sku

        Returns:
            Dictionary mapping SKU codes to features (guaranteed valid)
        """
        features_dict = {}

        for sku_code in sku_codes:
            try:
                features = FeatureEngineer.create_features_for_sku(
                    sku_code,
                    forecast_date,
                    **kwargs
                )
                features_dict[sku_code] = features if features else FeatureEngineer._get_default_features(forecast_date)
            except Exception as e:
                logger.error(f"Error creating features for {sku_code}: {str(e)}")
                features_dict[sku_code] = FeatureEngineer._get_default_features(forecast_date)

        return features_dict
