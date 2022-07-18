from datetime import datetime
from dagster import schedule
from DCF_data.jobs.news_jobs import company_news_pipeline


@schedule(
    job= company_news_pipeline,
    cron_schedule="0 0 * * 0",
    execution_timezone="America/Chicago",
)
def every_week(context):
    'run news pipeline every week on sunday at 00:00'
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    
    return {
        'ops': {''}
    }