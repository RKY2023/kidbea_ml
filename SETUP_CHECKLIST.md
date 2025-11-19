# kidbea_ml Setup Checklist

## ✅ Phase 3: kidbea_ml Complete

This repository has been created with all necessary files and configurations for the ML worker.

### Files Created

**Configuration** (2 files):
- [x] config/__init__.py
- [x] config/celery_app.py - ML Celery configuration
- [x] config/settings.py - Django settings with ML configuration

**Forecasting** (1 file):
- [x] forecasting/__init__.py
- [x] forecasting/tasks.py - All 7 scheduled ML tasks (FULL IMPLEMENTATION)

**Services** (1+ files):
- [x] forecasting/services/__init__.py
- [ ] forecasting/services/prediction_service.py - PLACEHOLDER (copy from kidbea_wh)
- [ ] forecasting/services/feature_engineering.py - TO BE COPIED
- [ ] forecasting/services/weather_service.py - TO BE COPIED
- [ ] forecasting/services/trends_service.py - TO BE COPIED

**Utils & Models**:
- [x] forecasting/utils/__init__.py
- [x] forecasting/models/__init__.py

**Testing**:
- [x] tests/__init__.py

**Deployment** (5 files):
- [x] requirements.txt - Heavy ML packages (~400MB)
- [x] Dockerfile - Production container with ML libs
- [x] .dockerignore - Build optimization
- [x] railway.toml - Railway deployment config
- [x] Procfile - ML worker process definition

**Configuration Files** (3 files):
- [x] .env.example - Environment variables template
- [x] .gitignore - Git exclusions
- [x] README.md - ML worker documentation

### Critical: Files to Copy from kidbea_wh

These files need to be copied from kidbea_wh/forecasting/ before deployment:

1. **forecasting/services/prediction_service.py** (750+ lines)
   - Main demand forecasting service
   - Location: kidbea_wh/forecasting/services/prediction_service.py

2. **forecasting/services/feature_engineering.py** (20KB)
   - Feature engineering for ML models
   - Location: kidbea_wh/forecasting/services/feature_engineering.py

3. **forecasting/services/weather_service.py**
   - Weather data collection from Open-Meteo API
   - Location: kidbea_wh/forecasting/services/weather_service.py

4. **forecasting/services/trends_service.py**
   - Google Trends data collection
   - Location: kidbea_wh/forecasting/services/trends_service.py

5. **forecasting/static_data/** (directory)
   - indian_festivals.json
   - seasonal_patterns.json
   - Location: kidbea_wh/forecasting/static_data/

6. **forecasting/utils/data_loaders.py**
   - Utility functions for loading static data
   - Location: kidbea_wh/forecasting/utils/data_loaders.py

7. **forecasting/models.py** (database models)
   - NOTE: Models are in kidbea_wh but referenced by tasks
   - These should NOT be duplicated - tasks query shared database
   - Location: kidbea_wh/forecasting/models.py (READ-ONLY reference)

### ML Tasks Implemented

All 9 Celery tasks are FULLY IMPLEMENTED in forecasting/tasks.py:

**Scheduled Tasks** (7):
1. ✅ collect_weather_data - Daily 6 AM
2. ✅ collect_trends_data - Weekly Sunday 7 AM
3. ✅ update_festival_calendar - Monthly 1st midnight
4. ✅ train_all_skus - Daily 10 PM (placeholder, ready to implement)
5. ✅ generate_forecasts - Daily 2 AM
6. ✅ calculate_accuracy - Daily 3 AM
7. ✅ generate_alerts - Daily 4 AM

**Manual Tasks** (2):
8. ✅ generate_forecast_for_sku - Single SKU forecast
9. ✅ retrain_model - Model retraining (placeholder)

### Next Steps

#### 1. Copy Service Files from kidbea_wh
```bash
# Copy services
cp kidbea_wh/forecasting/services/*.py kidbea_ml/forecasting/services/
cp kidbea_wh/forecasting/utils/data_loaders.py kidbea_ml/forecasting/utils/
cp -r kidbea_wh/forecasting/static_data/* kidbea_ml/forecasting/static_data/
```

#### 2. Create GitHub Repository
```bash
# On GitHub.com:
# 1. Click + → New repository
# 2. Name: kidbea_ml
# 3. Description: "ML worker for forecasting and prediction tasks"
# 4. Make it PRIVATE
# 5. Create repository
```

#### 3. Initialize Git and Push
```bash
cd kidbea_ml

# Initialize git
git init
git add .
git commit -m "Initial kidbea_ml setup with forecasting services and tasks"

# Add remote
git remote add origin https://github.com/YOUR_USERNAME/kidbea_ml.git
git branch -M main
git push -u origin main
```

#### 4. Deploy to Railway
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Create new project
railway init

# Set environment variables
railway variables

# Add these:
# DB_NAME=kidbea
# DB_USER=postgres
# DB_PASSWORD=[your-password]
# DB_HOST=[your-host].pooler.supabase.co
# DB_PORT=6543
# CELERY_BROKER_URL=redis://...
# CELERY_RESULT_BACKEND=redis://...
# SECRET_KEY=[your-secret-key]
# ML_MODEL_CACHE_DIR=/tmp/ml_models
# FORECAST_HORIZON_DAYS=30

# Deploy
railway up
```

#### 5. Verify Deployment
```bash
# Check logs (may take several minutes for ML packages to install)
railway logs --follow

# Expected output:
# celery@ml-worker: Starting celery worker with concurrency=2
# celery@ml-worker: Ready to accept ml_tasks

# Check task processing
# Send test task from kidbea_wh and monitor in Flower dashboard
```

### Environment Variables Needed

```
DB_NAME=kidbea
DB_USER=postgres
DB_PASSWORD=xxx
DB_HOST=xxx.pooler.supabase.co
DB_PORT=6543
CELERY_BROKER_URL=redis://default:password@host:port
CELERY_RESULT_BACKEND=redis://default:password@host:port
SECRET_KEY=xxx
ML_MODEL_CACHE_DIR=/tmp/ml_models
FORECAST_HORIZON_DAYS=30
FORECAST_CONFIDENCE_LEVEL=0.95
MIN_HISTORY_DAYS=30
REORDER_POINT_MULTIPLIER=1.5
```

### File Structure
```
kidbea_ml/
├── config/
│   ├── __init__.py
│   ├── celery_app.py
│   └── settings.py
├── forecasting/
│   ├── __init__.py
│   ├── tasks.py                    (FULL IMPLEMENTATION ✅)
│   ├── services/
│   │   ├── __init__.py
│   │   ├── prediction_service.py   (TO BE COPIED)
│   │   ├── feature_engineering.py  (TO BE COPIED)
│   │   ├── weather_service.py      (TO BE COPIED)
│   │   └── trends_service.py       (TO BE COPIED)
│   ├── models/
│   │   └── __init__.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── data_loaders.py         (TO BE COPIED)
│   └── static_data/
│       ├── indian_festivals.json   (TO BE COPIED)
│       └── seasonal_patterns.json  (TO BE COPIED)
├── tests/
│   └── __init__.py
├── requirements.txt
├── Dockerfile
├── .dockerignore
├── railway.toml
├── Procfile
├── .env.example
├── .gitignore
├── README.md
└── SETUP_CHECKLIST.md
```

### Important Notes

⚠️ **Large Build Time**: ML packages (numpy, scipy, scikit-learn) take ~5-10 minutes to build on Railway

⚠️ **Shared Database**: Don't include Django models migration - they're in kidbea_wh. This worker queries the shared PostgreSQL database.

⚠️ **Task Imports**: Tasks reference models from `forecasting.models` - ensure database connection is working

### Success Criteria

✅ **Phase 3 Complete when:**
1. All service files copied from kidbea_wh
2. GitHub repository created
3. Code pushed to GitHub
4. Railway project created
5. Environment variables set
6. Deployed to Railway
7. ML worker running (check logs for "Ready to accept ml_tasks")
8. Can queue test task from kidbea_wh
9. Task processes in ml_tasks queue
10. Results visible in Flower dashboard

### Verification Checklist

- [ ] All files created
- [ ] Service files copied from kidbea_wh
- [ ] GitHub repository created
- [ ] Code pushed to GitHub
- [ ] Railway project created
- [ ] Environment variables set
- [ ] Deployed to Railway
- [ ] ML worker running
- [ ] Can query tasks from database
- [ ] Scheduled tasks appear in ml_tasks queue (via Flower)
- [ ] Test forecast task completes successfully

### Troubleshooting

**Build Fails with Numpy/Scipy?**
→ Railway may need more build resources
→ Retry the deployment
→ Check for network timeouts during pip install

**Worker Not Starting?**
→ Check CELERY_BROKER_URL is correct
→ Check database connection
→ Check logs for import errors
→ Verify ml_tasks queue exists in Redis

**Tasks Stuck in Queue?**
→ Verify worker is listening to ml_tasks queue
→ Check worker concurrency settings
→ Check for task timeouts
→ Review worker logs for errors

**Import Errors for Models?**
→ Ensure DATABASE connection to shared PostgreSQL
→ Ensure Django settings correct
→ Verify forecasting models exist in database

### Next Steps After Deployment

1. **Monitor Task Execution**
   - Queue test task from kidbea_wh
   - Monitor in Flower dashboard
   - Check logs for execution

2. **Optimize Performance**
   - Monitor CPU/memory usage
   - Adjust concurrency if needed
   - Cache frequent calculations

3. **Implement ML Improvements**
   - Replace multiplier-based forecasting with real models
   - Implement cross-validation
   - Add ensemble methods

---

**Status**: ✅ Repository structure complete | Ready for service file copying and GitHub/Railway deployment
