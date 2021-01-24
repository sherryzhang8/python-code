from copy import deepcopy
from datetime import datetime, timedelta

from airflow.alert_callback import OncallAlert
from airflow.dags.teams_dags import DataDAG, DataBackfillDAG
from airflow.operators import HiveExecOperator
from airflow.operators.sensors import RetryableExternalTaskSensor
from airflow.webhook import Channels

DEFAULT_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 20),
    'retries': 4,
    'retry_delay': timedelta(minutes=15),
    'enable_data_lineage': True,
}

dag = DataDAG(
    name='sd_requests_agg_daily',
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    concurrency=8,
    max_active_runs=8,
    alerts=OncallAlert(
        channel=Channels.ALERTS_DE,
        priority='low',
        no_retry_alert=True,
        escalation_chain=['sherryzhang'],
        notes="DAG for SD Requests",
    )
)


def run_workflow(dag, is_backfill=False):
    if not is_backfill:
        sd_requests_agg_sql = HiveExecOperator(
            hive_version="2.3",
            task_id="sd_requests_agg_sql",
            hql='sd_requests_agg.sql',
            dag=dag)

        wf_dw_hap_log = RetryableExternalTaskSensor.wait_for_all_hours(
            task_id='wf_dw_hap_log',
            external_dag_id='hap_log_hourly_v1',
            external_task_id='hap_log',
            dag=dag
        )

        wf_dw_aws_cloudfront_logs = RetryableExternalTaskSensor.wait_for_all_hours(
            task_id='wf_dw_aws_cloudfront_logs',
            external_dag_id='aws_cloudfront_logs_hourly',
            external_task_id='aws_cloudfront_logs_hourly',
            dag=dag
        )
        sd_requests_agg_sql.set_upstream([wf_dw_hap_log, wf_dw_aws_cloudfront_logs])

    else:
        sd_requests_agg_sql = HiveExecOperator(
            hive_version="2.3",
            task_id="sd_requests_agg_sql",
            hql='sd_requests_agg.sql',
            dag=dag)


# This is daily job
run_workflow(dag, is_backfill=False)

BACKFILL_ARGS = deepcopy(DEFAULT_ARGS)
BACKFILL_ARGS['start_date'] = datetime(2020, 8, 1)
BACKFILL_ARGS['end_date'] = datetime(2020, 8, 12)
BACKFILL_ARGS['params'] = {'cluster': 'backfill'}

sdi_requests_agg_backfill = DataBackfillDAG(
    name='sd_requests_agg_backfill_v9',
    default_args=BACKFILL_ARGS,
    schedule_interval="@daily",
    alerts=OncallAlertCallback(
        channel=Channels.ALERTS_BACKFILL,
        priority='low',
        no_retry_alert=True,
        escalation_chain=['sherryzhang'],
        notes="Backfill DAG for SD Requests",
    )
)

# Plan to backfill SDI Requests
run_workflow(sd_requests_agg_backfill, is_backfill=True)
