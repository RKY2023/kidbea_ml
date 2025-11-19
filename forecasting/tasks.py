from celery import shared_task
from django.utils import timezone
from django.db import transaction
import logging
import time

from forecasting.models import ExternalDataSource
from forecasting.services.weather_service import WeatherDataCollector, DEFAULT_LOCATIONS
from forecasting.services.trends_service import GoogleTrendsCollector, DEFAULT_KEYWORDS
from forecasting.utils.data_loaders import load_festival_calendar

logger = logging.getLogger(__name__)


# ==================== Data Collection Tasks ====================

@shared_task(bind=True, max_retries=3)
def collect_weather_data(self):
    """
    Collect weather data from Open-Meteo API (FREE, no key needed)
    Scheduled: Daily at 6 AM
    """
    try:
        logger.info("Starting weather data collection...")

        with WeatherDataCollector(timeout=15) as collector:
            weather_data = collector.collect_for_default_locations()

        collected_count = 0
        failed_count = 0

        for location_name, location_data in weather_data.items():
            if not location_data:
                logger.warning(f"No weather data for {location_name}")
                failed_count += 1
                continue

            try:
                location_info = location_data['location']
                current = location_data.get('current')
                forecast = location_data.get('forecast')

                # Store current weather
                if current:
                    ExternalDataSource.objects.update_or_create(
                        data_type='weather',
                        location_code=location_name,
                        data_date=timezone.now().date(),
                        defaults={
                            'value': current.get('temperature'),
                            'raw_data': current,
                            'source': 'open-meteo'
                        }
                    )
                    collected_count += 1

                # Store forecast data
                if forecast:
                    for day_forecast in forecast[:7]:  # Store 7-day forecast
                        forecast_date = day_forecast.get('date')
                        if forecast_date:
                            ExternalDataSource.objects.update_or_create(
                                data_type='weather_forecast',
                                location_code=location_name,
                                data_date=forecast_date,
                                defaults={
                                    'value': day_forecast.get('temperature_avg'),
                                    'raw_data': day_forecast,
                                    'source': 'open-meteo'
                                }
                            )

            except Exception as e:
                logger.error(f"Error storing weather data for {location_name}: {str(e)}")
                failed_count += 1

        logger.info(f"Weather collection completed: {collected_count} successful, {failed_count} failed")
        return {
            'status': 'success',
            'collected': collected_count,
            'failed': failed_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error collecting weather data: {exc}")
        # Retry with exponential backoff (5 min, 10 min, 15 min)
        raise self.retry(exc=exc, countdown=300 * (self.request.retries + 1))


@shared_task(bind=True, max_retries=3)
def collect_trends_data(self):
    """
    Collect Google Trends data using pytrends (FREE)
    Scheduled: Weekly on Sunday at 7 AM
    Rate limiting: 5+ second delays between requests to avoid throttling
    """
    try:
        logger.info("Starting Google Trends data collection...")

        collector = GoogleTrendsCollector()
        category_trends = collector.collect_default_categories(timeframe='today 3-m')

        stored_count = 0
        failed_count = 0

        for category, trends_data in category_trends.items():
            if not trends_data:
                logger.warning(f"No trends data for {category}")
                failed_count += 1
                continue

            try:
                # Store trends data
                ExternalDataSource.objects.update_or_create(
                    data_type='trends',
                    product_code=category,
                    data_date=timezone.now().date(),
                    location_code='IN',  # India
                    defaults={
                        'value': trends_data.get('data', {}).get('score', 50),
                        'raw_data': trends_data,
                        'source': 'google-trends'
                    }
                )
                stored_count += 1

            except Exception as e:
                logger.error(f"Error storing trends data for {category}: {str(e)}")
                failed_count += 1

            # Rate limiting: delay between requests
            time.sleep(2)

        logger.info(f"Trends collection completed: {stored_count} successful, {failed_count} failed")
        return {
            'status': 'success',
            'collected': stored_count,
            'failed': failed_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error collecting trends data: {exc}")
        raise self.retry(exc=exc, countdown=300 * (self.request.retries + 1))


@shared_task(bind=True, max_retries=3)
def update_festival_calendar(self):
    """
    Load festival and holiday calendar from static JSON data
    Scheduled: Monthly on 1st at midnight
    """
    try:
        logger.info("Starting festival calendar update...")

        festivals_data = load_festival_calendar(use_cache=False)
        updated_count = 0

        for festival in festivals_data.get('festivals', []):
            try:
                # Get this year and next year
                for year in [timezone.now().year, timezone.now().year + 1]:
                    festival_date_str = festival.get('dates', {}).get(str(year))

                    if festival_date_str:
                        ExternalDataSource.objects.update_or_create(
                            data_type='festival',
                            product_code=festival.get('name'),
                            data_date=festival_date_str,
                            location_code=festival.get('region', 'pan-india'),
                            defaults={
                                'value': festival.get('demand_multiplier', 1.0),
                                'raw_data': festival,
                                'source': 'static-data'
                            }
                        )
                        updated_count += 1

            except Exception as e:
                logger.error(f"Error updating festival {festival.get('name')}: {str(e)}")

        logger.info(f"Festival calendar update completed: {updated_count} records updated")
        return {
            'status': 'success',
            'updated': updated_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error updating festival calendar: {exc}")
        raise self.retry(exc=exc, countdown=300 * (self.request.retries + 1))


# ==================== Forecasting Tasks ====================

@shared_task(bind=True, max_retries=2)
def train_all_skus(self):
    """
    Train forecast models for all active SKUs
    Scheduled: Weekly on Sunday at 10 PM
    Note: Currently placeholder - full ML pipeline will be implemented next phase
    """
    try:
        logger.info("Starting model training for all SKUs...")

        # TODO: Implement actual ML training pipeline
        # Placeholder for future implementation:
        # 1. Load historical sales data
        # 2. Train multiple models (Moving Average, Exponential Smoothing, etc.)
        # 3. Evaluate performance on test set
        # 4. Save best model to ModelVersion

        logger.info("Model training completed (placeholder)")
        return {
            'status': 'success',
            'message': 'Model training scheduled for implementation',
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error training models: {exc}")
        raise self.retry(exc=exc, countdown=600 * (self.request.retries + 1))


@shared_task(bind=True, max_retries=2)
def generate_forecasts(self):
    """
    Generate demand forecasts for all SKUs using PredictionService
    Scheduled: Daily at 2 AM
    """
    try:
        logger.info("Starting forecast generation for all SKUs...")

        from variant.models import ProductVariant
        from forecasting.services.prediction_service import PredictionService

        # Get all active SKUs
        skus = ProductVariant.objects.filter(
            product__is_active=True
        ).values_list('sku', flat=True)[:1000]  # Limit to 1000 per run

        forecast_count = 0
        error_count = 0

        for sku in skus:
            try:
                forecast = PredictionService.get_demand_forecast(sku, days_ahead=30)
                if forecast:
                    forecast_count += 1
            except Exception as e:
                logger.warning(f"Error generating forecast for {sku}: {str(e)}")
                error_count += 1

        logger.info(f"Forecast generation completed: {forecast_count} successful, {error_count} failed")
        return {
            'status': 'success',
            'forecasts_generated': forecast_count,
            'errors': error_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error generating forecasts: {exc}")
        raise self.retry(exc=exc, countdown=600 * (self.request.retries + 1))


@shared_task(bind=True, max_retries=2)
def calculate_accuracy(self):
    """
    Calculate forecast accuracy metrics against actual sales
    Scheduled: Daily at 3 AM
    """
    try:
        logger.info("Starting accuracy calculation...")

        from forecasting.models import DemandForecast, ForecastAccuracy, HistoricalSalesDaily
        from datetime import timedelta

        # Find forecasts from yesterday
        yesterday = timezone.now().date() - timedelta(days=1)
        forecasts = DemandForecast.objects.filter(
            forecast_date=yesterday,
            actual_quantity__isnull=False
        )

        accuracy_count = 0
        error_count = 0

        for forecast in forecasts[:1000]:
            try:
                # Calculate error metrics
                predicted = forecast.predicted_quantity
                actual = forecast.actual_quantity

                if predicted == 0:
                    percentage_error = 100
                else:
                    percentage_error = abs(actual - predicted) / predicted * 100

                absolute_error = abs(actual - predicted)
                squared_error = (actual - predicted) ** 2

                # Store accuracy metrics
                ForecastAccuracy.objects.update_or_create(
                    product_variant=forecast.product_variant,
                    sku_code=forecast.sku_code,
                    metric_date=yesterday,
                    forecast_date=forecast.forecast_date,
                    days_ahead=forecast.days_ahead,
                    defaults={
                        'predicted_quantity': predicted,
                        'actual_quantity': actual,
                        'absolute_error': absolute_error,
                        'percentage_error': percentage_error,
                        'squared_error': squared_error,
                        'model_version': forecast.model_version,
                        'model_type': forecast.model_type
                    }
                )
                accuracy_count += 1

            except Exception as e:
                logger.warning(f"Error calculating accuracy for forecast {forecast.id}: {str(e)}")
                error_count += 1

        logger.info(f"Accuracy calculation completed: {accuracy_count} records, {error_count} errors")
        return {
            'status': 'success',
            'accuracy_records': accuracy_count,
            'errors': error_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error calculating accuracy: {exc}")
        raise self.retry(exc=exc, countdown=600 * (self.request.retries + 1))


@shared_task(bind=True, max_retries=2)
def generate_alerts(self):
    """
    Generate inventory alerts based on forecasts and current stock
    Scheduled: Daily at 4 AM
    """
    try:
        logger.info("Starting alert generation...")

        from variant.models import ProductVariant
        from forecasting.services.prediction_service import PredictionService
        from forecasting.models import InventoryAlert

        # Get all active products
        variants = ProductVariant.objects.filter(product__is_active=True)[:500]

        alert_count = 0
        error_count = 0

        for variant in variants:
            try:
                # Get forecast
                forecast = PredictionService.get_demand_forecast(variant.sku, days_ahead=14)

                if not forecast:
                    continue

                current_stock = forecast.get('current_stock', 0)
                days_until_stockout = forecast.get('days_until_stockout', 999)
                next_7d_demand = sum(
                    f['predicted_quantity'] for f in forecast['forecasts'][:7]
                )

                # Determine alert type and severity
                alert_type = None
                severity = None

                if days_until_stockout < 3:
                    alert_type = 'stockout_warning'
                    severity = 'critical'
                elif days_until_stockout < 7:
                    alert_type = 'low_stock'
                    severity = 'warning'
                elif current_stock < (next_7d_demand / 7 * 1.5):
                    alert_type = 'low_stock'
                    severity = 'info'

                if alert_type:
                    alert, created = InventoryAlert.objects.update_or_create(
                        product_variant=variant,
                        sku_code=variant.sku,
                        defaults={
                            'alert_type': alert_type,
                            'severity': severity,
                            'status': 'active',
                            'current_stock': current_stock,
                            'predicted_daily_demand': next_7d_demand / 7,
                            'days_until_stockout': days_until_stockout,
                            'recommended_reorder_quantity': int(next_7d_demand * 1.5)
                        }
                    )
                    alert_count += 1

            except Exception as e:
                logger.warning(f"Error generating alerts for {variant.sku}: {str(e)}")
                error_count += 1

        logger.info(f"Alert generation completed: {alert_count} alerts, {error_count} errors")
        return {
            'status': 'success',
            'alerts_generated': alert_count,
            'errors': error_count,
            'timestamp': timezone.now().isoformat()
        }

    except Exception as exc:
        logger.error(f"Error generating alerts: {exc}")
        raise self.retry(exc=exc, countdown=600 * (self.request.retries + 1))


# ==================== Manual/Triggered Tasks ====================

@shared_task
def generate_forecast_for_sku(sku_code):
    """
    Generate forecast for a specific SKU (can be triggered manually)
    """
    try:
        from forecasting.services.prediction_service import PredictionService

        logger.info(f"Generating forecast for SKU: {sku_code}")

        forecast = PredictionService.get_demand_forecast(sku_code, days_ahead=30)

        if forecast:
            logger.info(f"Forecast generated for SKU: {sku_code}")
            return {
                'status': 'success',
                'sku': sku_code,
                'days_forecasted': len(forecast.get('forecasts', []))
            }
        else:
            return {
                'status': 'error',
                'sku': sku_code,
                'message': f'Could not generate forecast for {sku_code}'
            }

    except Exception as exc:
        logger.error(f"Error generating forecast for {sku_code}: {exc}")
        return {
            'status': 'error',
            'sku': sku_code,
            'message': str(exc)
        }


@shared_task
def retrain_model(model_type=None):
    """
    Manually trigger model retraining
    Note: Placeholder for full ML training pipeline
    """
    try:
        logger.info(f"Retraining model: {model_type or 'all'}")

        # TODO: Implement model retraining
        # This will be implemented when ML models are added

        logger.info("Model retraining scheduled for implementation")
        return {
            'status': 'success',
            'model_type': model_type or 'all',
            'message': 'Model retraining scheduled for future implementation'
        }

    except Exception as exc:
        logger.error(f"Error retraining model: {exc}")
        return {
            'status': 'error',
            'message': str(exc)
        }
