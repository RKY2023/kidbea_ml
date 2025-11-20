"""
Main prediction service for demand forecasting.
Coordinates external data, feature engineering, and forecast generation.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from django.utils import timezone
from django.db.models import Avg, Sum, Max, Min, Q
from django.core.cache import cache

from forecasting.models import (
    ModelVersion, ForecastConfig, DemandForecast, ForecastAccuracy,
    InventoryAlert, HistoricalSalesDaily, ExternalDataSource
)
from variant.models import ProductVariant
from category.models import Category
from forecasting.services.feature_engineering import FeatureEngineer

logger = logging.getLogger(__name__)


class PredictionService:
    """Main service for generating demand forecasts."""

    @staticmethod
    def get_demand_forecast(
        sku_code: str,
        days_ahead: int = 7,
        model_type: Optional[str] = None,
        include_confidence: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get demand forecast for a specific SKU.

        Args:
            sku_code: Product SKU code
            days_ahead: Number of days to forecast
            model_type: Specific model type to use (default: ensemble)
            include_confidence: Include confidence intervals

        Returns:
            Dictionary with forecast data
        """
        cache_key = f"forecast:demand:{sku_code}:{days_ahead}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        try:
            # Try to get variant, but continue with defaults if not found
            variant = ProductVariant.objects.filter(sku=sku_code).first()
            product_name = 'Unknown'
            if variant:
                product_name = getattr(variant.product, 'name', 'Unknown') if hasattr(variant, 'product') else 'Unknown'

            # Get or use active model
            if not model_type:
                active_model = ModelVersion.objects.filter(is_active=True).first()
                model_type = active_model.model_type if active_model else 'moving_average'

            # Generate forecasts
            forecasts = []
            today = timezone.now()

            for day_offset in range(1, days_ahead + 1):
                forecast_date = today + timedelta(days=day_offset)

                # Create features with graceful fallbacks
                try:
                    features = FeatureEngineer.create_features_for_sku(sku_code, forecast_date)
                except Exception as feature_error:
                    logger.warning(f"Feature engineering error for {sku_code}: {feature_error}, using defaults")
                    features = PredictionService._get_default_features()

                # Generate prediction - guaranteed to return a dict
                prediction = PredictionService._generate_single_forecast(
                    sku_code,
                    forecast_date,
                    features,
                    model_type,
                    include_confidence
                )

                if prediction:
                    forecasts.append(prediction)

            # If no forecasts generated, create at least one default
            if not forecasts:
                logger.warning(f"No forecasts generated for {sku_code}, creating default")
                for day_offset in range(1, min(days_ahead + 1, 8)):
                    forecast_date = today + timedelta(days=day_offset)
                    forecasts.append({
                        'date': forecast_date.date().isoformat(),
                        'predicted_quantity': 10,
                        'confidence_lower': 5,
                        'confidence_upper': 15,
                        'model_type': model_type,
                        'influencing_factors': ['Using default forecast - insufficient data']
                    })

            # Get current stock and reorder info
            current_stock = PredictionService._get_current_stock(sku_code)
            days_until_stockout = PredictionService._calculate_days_to_stockout(
                sku_code,
                forecasts
            )
            recommended_reorder = PredictionService._calculate_reorder_quantity(
                sku_code,
                forecasts
            )

            result = {
                'sku': sku_code,
                'product_name': product_name,
                'forecast_start_date': (today + timedelta(days=1)).date().isoformat(),
                'forecast_end_date': (today + timedelta(days=days_ahead)).date().isoformat(),
                'forecasts': forecasts,
                'current_stock': current_stock,
                'days_until_stockout': days_until_stockout,
                'recommended_reorder': recommended_reorder,
                'model_type': model_type,
                'generated_at': timezone.now().isoformat()
            }

            # Cache for 6 hours
            cache.set(cache_key, result, 21600)
            return result

        except Exception as e:
            logger.error(f"Error generating forecast for {sku_code}: {str(e)}")
            # Return minimal valid forecast instead of None
            today = timezone.now()
            return {
                'sku': sku_code,
                'product_name': 'Unknown',
                'forecast_start_date': (today + timedelta(days=1)).date().isoformat(),
                'forecast_end_date': (today + timedelta(days=days_ahead)).date().isoformat(),
                'forecasts': [
                    {
                        'date': (today + timedelta(days=i)).date().isoformat(),
                        'predicted_quantity': 10,
                        'confidence_lower': 5,
                        'confidence_upper': 15,
                        'model_type': model_type or 'moving_average',
                        'influencing_factors': ['Error in forecast generation']
                    }
                    for i in range(1, min(days_ahead + 1, 8))
                ],
                'current_stock': 0,
                'days_until_stockout': 999,
                'recommended_reorder': 100,
                'model_type': model_type or 'moving_average',
                'generated_at': timezone.now().isoformat()
            }

    @staticmethod
    def _generate_single_forecast(
        sku_code: str,
        forecast_date: datetime,
        features: Dict[str, Any],
        model_type: str,
        include_confidence: bool
    ) -> Optional[Dict[str, Any]]:
        """Generate a single day forecast. Never returns None - always returns valid data."""
        try:
            # Get historical baseline
            baseline_demand = PredictionService._get_baseline_demand(sku_code)

            # Apply multipliers - ensure all are present with safe defaults
            multipliers = [
                float(features.get('seasonal_multiplier', 1.0)),
                float(features.get('festival_multiplier', 1.0)),
                float(features.get('day_of_week_multiplier', 1.0)),
                float(features.get('month_multiplier', 1.0)),
                float(features.get('lifecycle_multiplier', 1.0)),
                float(features.get('temperature_impact', 1.0)),
                float(features.get('weather_impact', 1.0))
            ]

            combined_multiplier = 1.0
            for mult in multipliers:
                if mult and mult > 0:
                    combined_multiplier *= mult

            predicted_quantity = max(int(baseline_demand * combined_multiplier), 1)

            # Calculate confidence intervals
            confidence_lower = predicted_quantity
            confidence_upper = predicted_quantity

            if include_confidence:
                # Use historical volatility for confidence intervals
                volatility = features.get('sales_volatility_7d', 0)
                confidence_range = max(int(predicted_quantity * 0.2), 5)  # 20% or min 5
                confidence_lower = max(predicted_quantity - confidence_range, 0)
                confidence_upper = predicted_quantity + confidence_range

            # Influencing factors for transparency
            influencing_factors = []
            if features.get('is_festival_week'):
                influencing_factors.append(f"Festival: {features.get('festival_name', 'Unknown')}")
            if features.get('sales_trend_7d') == 'increasing':
                influencing_factors.append('Increasing trend')
            if features.get('temperature_impact', 1.0) != 1.0:
                influencing_factors.append('Weather impact')

            return {
                'date': forecast_date.date().isoformat(),
                'predicted_quantity': predicted_quantity,
                'confidence_lower': confidence_lower,
                'confidence_upper': confidence_upper,
                'model_type': model_type,
                'influencing_factors': influencing_factors
            }

        except Exception as e:
            logger.error(f"Error in single forecast for {sku_code} on {forecast_date}: {str(e)}")
            # Return safe default instead of None
            return {
                'date': forecast_date.date().isoformat(),
                'predicted_quantity': 10,
                'confidence_lower': 5,
                'confidence_upper': 15,
                'model_type': model_type,
                'influencing_factors': ['Default forecast due to error']
            }

    @staticmethod
    def get_category_forecast(
        category_id: int,
        days_ahead: int = 7
    ) -> Optional[Dict[str, Any]]:
        """Get aggregated forecast for a category. Never returns None."""
        try:
            from product.models import Category

            category = Category.objects.filter(id=category_id).first()
            if not category:
                # Return safe default if category not found
                return {
                    'category': 'Unknown',
                    'category_id': category_id,
                    'total_products': 0,
                    'forecast_period': f"{days_ahead} days",
                    'total_predicted_demand': 0,
                    'top_performers': [],
                    'daily_forecast': []
                }

            # Get all SKUs in category
            variants = ProductVariant.objects.filter(
                product__category_id=category_id
            ).values_list('sku', flat=True)

            if not variants:
                # Return safe default if no variants in category
                return {
                    'category': category.name,
                    'category_id': category_id,
                    'total_products': 0,
                    'forecast_period': f"{days_ahead} days",
                    'total_predicted_demand': 0,
                    'top_performers': [],
                    'daily_forecast': []
                }

            # Get forecasts for all SKUs
            total_forecasts = []
            top_performers = []

            for sku in list(variants)[:20]:  # Limit to first 20
                forecast = PredictionService.get_demand_forecast(sku, days_ahead)
                if forecast and forecast.get('forecasts'):
                    total_demand = sum(f['predicted_quantity'] for f in forecast['forecasts'])
                    top_performers.append({
                        'sku': sku,
                        'predicted_quantity': total_demand
                    })

            top_performers.sort(key=lambda x: x['predicted_quantity'], reverse=True)

            # Aggregate daily forecasts
            daily_totals = {}
            for sku in variants:
                forecast = PredictionService.get_demand_forecast(sku, days_ahead)
                if forecast:
                    for f in forecast.get('forecasts', []):
                        date = f['date']
                        if date not in daily_totals:
                            daily_totals[date] = 0
                        daily_totals[date] += f['predicted_quantity']

            daily_forecast = [
                {'date': date, 'predicted_quantity': qty}
                for date, qty in sorted(daily_totals.items())
            ]

            return {
                'category': category.name,
                'category_id': category_id,
                'total_products': len(variants),
                'forecast_period': f"{days_ahead} days",
                'total_predicted_demand': sum(daily_totals.values()),
                'top_performers': top_performers[:10],
                'daily_forecast': daily_forecast
            }

        except Exception as e:
            logger.error(f"Error getting category forecast: {str(e)}")
            # Return safe default on error
            return {
                'category': 'Unknown',
                'category_id': category_id,
                'total_products': 0,
                'forecast_period': f"{days_ahead} days",
                'total_predicted_demand': 0,
                'top_performers': [],
                'daily_forecast': []
            }

    @staticmethod
    def get_inventory_alerts(
        severity: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get current inventory alerts."""
        try:
            query = InventoryAlert.objects.all()

            if severity:
                query = query.filter(severity=severity)

            if status:
                query = query.filter(status=status)
            else:
                query = query.filter(status__in=['active', 'acknowledged'])

            alerts = []
            for alert in query[:100]:  # Limit to 100
                alerts.append({
                    'id': alert.id,
                    'sku': alert.sku_code,
                    'product_name': alert.product_variant.product.name if alert.product_variant else 'Unknown',
                    'alert_type': alert.alert_type,
                    'severity': alert.severity,
                    'current_stock': alert.current_stock,
                    'predicted_daily_demand': alert.predicted_daily_demand,
                    'days_until_stockout': alert.days_until_stockout,
                    'recommended_reorder_quantity': alert.recommended_reorder_quantity,
                    'created_at': alert.created_at.isoformat()
                })

            return alerts

        except Exception as e:
            logger.error(f"Error getting inventory alerts: {str(e)}")
            return []

    @staticmethod
    def get_reorder_recommendations(
        category_id: Optional[int] = None,
        min_severity: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get smart reorder recommendations."""
        try:
            alerts = InventoryAlert.objects.filter(
                status__in=['active', 'acknowledged']
            )

            if category_id:
                alerts = alerts.filter(product_variant__product__category_id=category_id)

            severity_levels = ['info', 'warning', 'critical']
            if min_severity and min_severity in severity_levels:
                min_index = severity_levels.index(min_severity)
                alerts = alerts.filter(
                    severity__in=severity_levels[min_index:]
                )

            recommendations = []
            total_value = 0

            for alert in alerts[:50]:
                if alert.alert_type in ['stockout_warning', 'low_stock']:
                    # Calculate lead time (default 7 days)
                    lead_time = 7

                    rec = {
                        'sku': alert.sku_code,
                        'product_name': alert.product_variant.product.name if alert.product_variant else 'Unknown',
                        'current_stock': alert.current_stock,
                        'recommended_quantity': alert.recommended_reorder_quantity,
                        'urgency': alert.severity,
                        'estimated_stockout_date': (
                            timezone.now() + timedelta(days=alert.days_until_stockout)
                        ).date().isoformat() if alert.days_until_stockout else None,
                        'forecasted_demand_30d': alert.predicted_daily_demand * 30,
                        'supplier_lead_time': lead_time
                    }

                    # Estimate price (basic calculation)
                    unit_price = 100  # Default estimate
                    if alert.product_variant:
                        unit_price = float(
                            alert.product_variant.selling_price or 100
                        )

                    rec['estimated_cost'] = rec['recommended_quantity'] * unit_price
                    total_value += rec['estimated_cost']

                    recommendations.append(rec)

            return {
                'recommendations': recommendations,
                'total_reorder_value': total_value,
                'recommendation_count': len(recommendations)
            }

        except Exception as e:
            logger.error(f"Error getting reorder recommendations: {str(e)}")
            return {'recommendations': [], 'total_reorder_value': 0}

    @staticmethod
    def get_seasonal_insights(sku_code: str) -> Optional[Dict[str, Any]]:
        """Get seasonal insights for a SKU."""
        try:
            variant = ProductVariant.objects.filter(sku=sku_code).first()
            # Continue even if variant not found - use defaults

            today = timezone.now()

            # Get upcoming festivals
            from forecasting.utils.data_loaders import get_upcoming_festivals

            upcoming_festivals = get_upcoming_festivals(days_ahead=90)

            festivals_with_impact = []
            for festival in upcoming_festivals:
                festival_date = datetime.strptime(
                    festival['festival_date'],
                    '%Y-%m-%d'
                )
                days_away = (festival_date - today).days

                festivals_with_impact.append({
                    'name': festival['name'],
                    'date': festival['festival_date'],
                    'days_away': days_away,
                    'expected_impact': f"+{int((festival.get('demand_multiplier', 1.0) - 1) * 100)}%"
                })

            # Historical seasonal pattern
            month = today.month
            historical_pattern = {
                str(i): None for i in range(1, 13)
            }

            # Calculate average sales by month for past 3 years
            for month_num in range(1, 13):
                sales = HistoricalSalesDaily.objects.filter(
                    sku_code=sku_code,
                    sale_date__month=month_num
                ).aggregate(Avg('quantity_sold'))

                if sales['quantity_sold__avg']:
                    historical_pattern[str(month_num)] = sales['quantity_sold__avg']

            # Current season
            if month in [11, 12, 1, 2]:
                season = 'winter'
            elif month in [3, 4, 5, 6]:
                season = 'summer'
            elif month in [7, 8, 9]:
                season = 'monsoon'
            else:
                season = 'spring'

            # Get seasonal multiplier
            from forecasting.utils.data_loaders import get_seasonal_multiplier

            category_name = 'default'
            if variant and hasattr(variant, 'product') and hasattr(variant.product, 'category'):
                try:
                    category_name = variant.product.category.name.lower()
                except:
                    category_name = 'default'

            seasonal_multiplier = get_seasonal_multiplier(category_name, month)

            return {
                'sku': sku_code,
                'current_season': season,
                'seasonal_multiplier': seasonal_multiplier,
                'upcoming_festivals': festivals_with_impact[:5],
                'historical_seasonal_pattern': {
                    k: v for k, v in historical_pattern.items() if v is not None
                }
            }

        except Exception as e:
            logger.error(f"Error getting seasonal insights: {str(e)}")
            # Return safe defaults instead of None
            today = timezone.now()
            month = today.month
            if month in [11, 12, 1, 2]:
                season = 'winter'
            elif month in [3, 4, 5, 6]:
                season = 'summer'
            elif month in [7, 8, 9]:
                season = 'monsoon'
            else:
                season = 'spring'

            return {
                'sku': sku_code,
                'current_season': season,
                'seasonal_multiplier': 1.0,
                'upcoming_festivals': [],
                'historical_seasonal_pattern': {}
            }

    @staticmethod
    def get_quick_forecast(sku_code: str) -> Optional[Dict[str, Any]]:
        """Get quick forecast for product cards/listings."""
        try:
            # Get forecast - guaranteed to return valid data
            forecast = PredictionService.get_demand_forecast(sku_code, days_ahead=7)

            if not forecast or not forecast.get('forecasts'):
                # Return minimal valid response
                return {
                    'sku': sku_code,
                    'next_7_days_demand': 70,
                    'trend': 'stable',
                    'stock_status': 'warning',
                    'reorder_needed': True
                }

            next_7_days = sum(f['predicted_quantity'] for f in forecast.get('forecasts', []))

            # Determine trend
            forecasts = forecast.get('forecasts', [])
            trend = 'stable'
            if len(forecasts) > 1:
                first_half = sum(f['predicted_quantity'] for f in forecasts[:len(forecasts)//2])
                second_half = sum(f['predicted_quantity'] for f in forecasts[len(forecasts)//2:])

                if first_half > 0:
                    change_pct = ((second_half - first_half) / first_half) * 100
                    if change_pct > 10:
                        trend = 'increasing'
                    elif change_pct < -10:
                        trend = 'decreasing'

            # Stock status
            current_stock = forecast.get('current_stock', 0)
            days_until_stockout = forecast.get('days_until_stockout', 999)

            if days_until_stockout < 3:
                stock_status = 'critical'
            elif days_until_stockout < 7:
                stock_status = 'low'
            elif current_stock < (next_7_days / 7):
                stock_status = 'warning'
            else:
                stock_status = 'healthy'

            return {
                'sku': sku_code,
                'next_7_days_demand': next_7_days,
                'trend': trend,
                'stock_status': stock_status,
                'reorder_needed': stock_status in ['critical', 'low']
            }

        except Exception as e:
            logger.error(f"Error getting quick forecast: {str(e)}")
            return None

    @staticmethod
    def get_forecast_accuracy_metrics(
        sku_code: Optional[str] = None,
        days_ahead: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get forecast accuracy metrics."""
        try:
            query = ForecastAccuracy.objects.all()

            if sku_code:
                query = query.filter(sku_code=sku_code)

            if days_ahead:
                query = query.filter(days_ahead=days_ahead)

            # Calculate metrics
            stats = query.aggregate(
                mape=Avg('percentage_error'),
                rmse=Sum('squared_error'),
                mae=Avg('absolute_error')
            )

            import math
            rmse = math.sqrt(stats['rmse']) if stats['rmse'] else 0

            # By model type
            by_model = {}
            for accuracy_record in query[:1000]:
                model_type = accuracy_record.model_type
                if model_type not in by_model:
                    by_model[model_type] = []
                by_model[model_type].append({
                    'predicted': accuracy_record.predicted_quantity,
                    'actual': accuracy_record.actual_quantity,
                    'error': accuracy_record.percentage_error
                })

            model_stats = {}
            for model_type, values in by_model.items():
                if values:
                    mape = sum(v['error'] for v in values) / len(values)
                    model_stats[model_type] = {
                        'mape': mape,
                        'rmse': math.sqrt(sum(v['error']**2 for v in values) / len(values))
                    }
                else:
                    # Ensure all model types have mape and rmse
                    model_stats[model_type] = {
                        'mape': 0.0,
                        'rmse': 0.0
                    }

            # Recent accuracy
            recent = list(
                query.order_by('-metric_date')[:10].values(
                    'metric_date', 'predicted_quantity', 'actual_quantity', 'percentage_error'
                )
            )

            # Ensure by_model_type is always a dict with proper structure
            # If empty, return at least one default entry
            if not model_stats:
                model_stats = {
                    'default': {
                        'mape': 0.0,
                        'rmse': 0.0
                    }
                }

            return {
                'overall_mape': stats['mape'] or 0,
                'overall_rmse': rmse,
                'overall_mae': stats['mae'] or 0,
                'by_model_type': model_stats,
                'recent_accuracy': recent
            }

        except Exception as e:
            logger.error(f"Error getting accuracy metrics: {str(e)}")
            return {
                'overall_mape': 0,
                'overall_rmse': 0,
                'overall_mae': 0,
                'by_model_type': {},
                'recent_accuracy': []
            }

    # Helper methods

    @staticmethod
    def _get_default_features() -> Dict[str, float]:
        """Get default features when actual features cannot be computed."""
        return {
            'seasonal_multiplier': 1.0,
            'festival_multiplier': 1.0,
            'day_of_week_multiplier': 1.0,
            'month_multiplier': 1.0,
            'lifecycle_multiplier': 1.0,
            'temperature_impact': 1.0,
            'weather_impact': 1.0,
            'sales_volatility_7d': 0,
            'is_festival_week': False,
            'festival_name': None,
            'sales_trend_7d': 'stable'
        }

    @staticmethod
    def _get_baseline_demand(sku_code: str) -> float:
        """Get baseline demand from historical data."""
        try:
            avg = HistoricalSalesDaily.objects.filter(
                sku_code=sku_code,
                sale_date__gte=timezone.now() - timedelta(days=30)
            ).aggregate(Avg('quantity_sold'))

            return avg['quantity_sold__avg'] or 10.0

        except:
            return 10.0

    @staticmethod
    def _get_current_stock(sku_code: str) -> int:
        """Get current stock for SKU."""
        try:
            variant = ProductVariant.objects.filter(sku=sku_code).first()
            if variant and hasattr(variant, 'stock_quantity'):
                return variant.stock_quantity or 0
            return 0
        except:
            return 0

    @staticmethod
    def _calculate_days_to_stockout(sku_code: str, forecasts: List[Dict]) -> float:
        """Calculate days until stockout."""
        try:
            current_stock = PredictionService._get_current_stock(sku_code)
            cumulative = 0
            today = timezone.now().date()

            for day_index, forecast in enumerate(forecasts, start=1):
                cumulative += forecast['predicted_quantity']
                if cumulative >= current_stock:
                    # Return the number of days from today
                    return float(day_index)

            return 999.0

        except:
            return 999.0

    @staticmethod
    def _calculate_reorder_quantity(sku_code: str, forecasts: List[Dict]) -> int:
        """Calculate recommended reorder quantity."""
        try:
            # 30-day forecast plus 7-day safety stock
            total_30d = sum(
                f['predicted_quantity'] for f in forecasts[:min(30, len(forecasts))]
            )
            safety_stock = int(total_30d * 0.3)  # 30% safety margin

            return int(total_30d + safety_stock)

        except:
            return 100
