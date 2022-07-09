from dagster import repository
from DCF_data.jobs.news_jobs import (
    company_news_pipeline
)


@repository
def news_dags():
    '''    The repository definition for this news_dags Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    '''

    jobs = [company_news_pipeline]
    # chedules = [my_hourly_schedule]
    # sensors = [my_sensor]

    return jobs


if __name__ == '__main__':
    None