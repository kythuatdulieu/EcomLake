from ..job import etl_pipeline_job
from dagster import ScheduleDefinition

'''

Crontab Syntax
+---------------- minute (0 - 59)
|  +------------- hour (0 - 23)
|  |  +---------- day of month (1 - 31)
|  |  |  +------- month (1 - 12)
|  |  |  |  +---- day of week (0 - 6) (Sunday is 0 or 7)
|  |  |  |  |
*  *  *  *  *  command to be executed

* means all values are acceptable

'''

daily_etl_schedule = ScheduleDefinition(
    job=etl_pipeline_job,
    cron_schedule="30 00 * * *", 
    execution_timezone="Asia/Ho_Chi_Minh",
)







# Path: etl_pipeline/etl_pipeline/schedule/__init__.py