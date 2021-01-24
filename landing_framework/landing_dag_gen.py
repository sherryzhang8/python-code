from slack.airflow.dags import SlackDAG
from slack.airflow.operators.livy_operators import LivyPySparkOperator
from slack.airflow.operators.sensors.retryable_sensors import RetryableExternalTaskSensor
from slack.airflow.operators.landing_framework.landing_framework_execution_operators import LandingFrameworkHiveOperator, LandingFrameworkPrestoOperator, LandingFrameworkVersionExpiryOperator
from slack.airflow.operators.landing_framework.landing_framework_metadata_operators import LandingFrameworkMetadataOperator
from slack.airflow.operators.landing_framework.config import LandingFrameworkConfig
from slack.airflow.operators.landing_framework.configuration_utils import read_yaml_configuration, get_dependency_task
from slack.airflow.operators.landing_framework.util import get_files_in_directory
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
import os
import logging
import traceback

BASE_PATH = os.path.dirname(os.path.abspath(__file__))


def get_yaml_configuration_files(configuration_type):
    """
    Returns a complete list of YAML configuration file paths for the given configuration type
    """
    # Validate that the configuration type is a known type
    if configuration_type not in LandingFrameworkConfig.CONFIGURATION_TYPES:
        raise Exception("'{}' is not a valid configuration type".format(configuration_type))

    # Setup the configuration directory
    configuration_dir = os.path.join(
        BASE_PATH, LandingFrameworkConfig.BASE_DIR_CONFIGURATION, configuration_type
    )

    # Get the list of YAML configuration files from the configuration directory
    configuration_files = get_files_in_directory(
        directory_path=configuration_dir, recursive=True, file_types='yaml'
    )

    return configuration_files


def generate_backfill_landing_dag(dag_name, configuration_metadata):
    # Populate default arguments with any overrides from configuration metadata
    # TODO: Add ability to override
    default_args = {
        'owner': '#team-dma',
        'depends_on_past': True,
        'wait_for_downstream': True,
        'start_date': configuration_metadata['backfill_date'],
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'params': {
            'cluster': 'dev-analytics'
        },
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
    }

    dag = SlackDAG(name=dag_name, default_args=default_args, schedule_interval='@once',
                   max_active_runs=1)

    # Extract metadata from the source, and pass into XCOM for further tasks to use to dynamically
    # generate ETL
    generate_metadata = LandingFrameworkMetadataOperator(
        task_id=LandingFrameworkConfig.GENERATE_METADATA_TASK_NAME,
        configuration_metadata=configuration_metadata,
        backfill=True,
        dag=dag
    )

    ddl = LandingFrameworkHiveOperator(
        task_id='ddl',
        configuration_metadata=configuration_metadata,
        landing_stage='ddl',
        backfill=True,
        dag=dag
    )
    ddl.set_upstream(generate_metadata)

    load_backfill = LivyPySparkOperator(
        task_id='load_backfill',
        py_entrypoint=os.path.join(
            LandingFrameworkConfig.BASE_DIR_TEMPLATE_ETL_LOAD, 'spark_load_s3_backfill.py'
        ),
        job_args=[
            '--source_name=%s' % configuration_metadata['source_name'],
            '--table_name=%s' % configuration_metadata['table_name'],
            '--data_date=%s' % configuration_metadata['backfill_date'].strftime('%Y-%m-%d'),
            '--target_s3_bucket=%s' % LandingFrameworkConfig.TARGET_S3_BUCKET,
            '--metadata_s3_path=%s' % LandingFrameworkConfig.METADATA_S3_PATH
        ],
        deploy_mode='client',
        dag=dag
    )
    load_backfill.set_upstream(ddl)

    landing = LandingFrameworkHiveOperator(
        task_id='landing',
        configuration_metadata=configuration_metadata,
        landing_stage='landing',
        backfill=True,
        dag=dag
    )
    landing.set_upstream(load_backfill)

    history = LandingFrameworkHiveOperator(
        task_id='history',
        configuration_metadata=configuration_metadata,
        landing_stage='history',
        backfill=True,
        dag=dag
    )
    history.set_upstream(landing)

    complete = DummyOperator(task_id=dag.dag_id + '_all_done', dag=dag)
    complete.set_upstream(history)

    return dag


def generate_landing_dag(dag_name, configuration_metadata):

    # Populate default arguments with any overrides from configuration metadata
    # TODO: Add ability to override
    default_args = {
        'owner': '#team-dma',
        'depends_on_past': True,
        'wait_for_downstream': True,
        'start_date': configuration_metadata['start_date'],
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'params': {
            'cluster': 'dev-analytics'
        },
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
    }

    # Set schedule interval
    if configuration_metadata['schedule_interval'] == 'daily':
        schedule_interval = '@{}'.format(configuration_metadata['schedule_interval'])
        schedule_execution_delta = timedelta(days=1)
    else:
        raise Exception(
            "Unsupported schedule interval: {}".format(configuration_metadata['schedule_interval'])
        )

    # Main DAG definition
    dag = SlackDAG(name=dag_name, default_args=default_args, schedule_interval=schedule_interval,
                   max_active_runs=1)

    # Check and wait for the complete previous run to have completed
    previous_run = RetryableExternalTaskSensor(
        task_id='check_previous',
        external_dag_id=dag_name,
        external_task_id=dag.dag_id + '_all_done',
        allowed_states=['success'],
        execution_delta=schedule_execution_delta,
        dag=dag
    )

    # Extract metadata from the source, and pass into XCOM for further tasks to use to dynamically generate ETL
    generate_metadata = LandingFrameworkMetadataOperator(
        task_id=LandingFrameworkConfig.GENERATE_METADATA_TASK_NAME,
        configuration_metadata=configuration_metadata,
        dag=dag
    )

    # Add dependencies if there are any to be added
    if configuration_metadata.get('dependencies'):
        for dependancy in configuration_metadata['dependencies']:
            task = get_dependency_task(dependency_metadata=dependancy, dag=dag)
            task.set_upstream(previous_run)
            generate_metadata.set_upstream(task)

    # Landing Task Stages
    ddl = LandingFrameworkHiveOperator(
        task_id='ddl', configuration_metadata=configuration_metadata, landing_stage='ddl', dag=dag
    )
    ddl.set_upstream(generate_metadata)

    load = LandingFrameworkHiveOperator(
        task_id='load',
        configuration_metadata=configuration_metadata,
        landing_stage='load',
        dag=dag
    )
    load.set_upstream(ddl)

    landing = LandingFrameworkHiveOperator(
        task_id='landing',
        configuration_metadata=configuration_metadata,
        landing_stage='landing',
        dag=dag
    )
    landing.set_upstream(load)

    history = LandingFrameworkHiveOperator(
        task_id='history',
        configuration_metadata=configuration_metadata,
        landing_stage='history',
        dag=dag
    )
    history.set_upstream(landing)

    history_version_expiry = LandingFrameworkVersionExpiryOperator(
        task_id='history_version_expiry',
        configuration_metadata=configuration_metadata,
        landing_stage='history',
        dag=dag
    )
    history_version_expiry.set_upstream(history)

    current = LandingFrameworkHiveOperator(
        task_id='current',
        configuration_metadata=configuration_metadata,
        landing_stage='current',
        dag=dag
    )
    current.set_upstream(history)

    current_version_expiry = LandingFrameworkVersionExpiryOperator(
        task_id='current_version_expiry',
        configuration_metadata=configuration_metadata,
        landing_stage='current',
        dag=dag
    )
    current_version_expiry.set_upstream(current)

    snapshot_view_hive = LandingFrameworkHiveOperator(
        task_id='snapshot_view_hive',
        configuration_metadata=configuration_metadata,
        landing_stage='snapshot',
        hive_version='2.3',
        dag=dag
    )
    snapshot_view_hive.set_upstream(current)

    snapshot_view_presto = LandingFrameworkPrestoOperator(
        task_id='snapshot_view_presto',
        configuration_metadata=configuration_metadata,
        landing_stage='snapshot',
        dag=dag
    )
    snapshot_view_presto.set_upstream(snapshot_view_hive)

    complete = DummyOperator(task_id=dag.dag_id + '_all_done', dag=dag)
    complete.set_upstream(snapshot_view_presto)

    return dag


# Dynamically generate dags from YAML Configurations
configuration_files = get_yaml_configuration_files('landing')
for config_file in configuration_files:
    try:
        logging.debug("Attempting to parse YAML Configuration: {}".format(config_file))
        config = read_yaml_configuration(os.path.join(config_file))

        # Don't create the dag for daily run or backfill if it is disabled in yaml
        if not config['enabled']:
            continue

        # Generate Standard Dag
        dag_id = 'landing_framework_{source_name}_{table_name}_v{version}'.format(
            source_name=config['source_name'],
            table_name=config['table_name'],
            version=config['version']
        )
        globals()[dag_id] = generate_landing_dag(dag_name=dag_id, configuration_metadata=config)

        # Generate Backfill Dag
        if config.get('backfill_date'):
            backfill_dag_id = 'landing_framework_{source_name}_{table_name}_backfill_v{version}'.format(
                source_name=config['source_name'],
                table_name=config['table_name'],
                version=config['version']
            )
            globals()[backfill_dag_id] = generate_backfill_landing_dag(
                dag_name=backfill_dag_id, configuration_metadata=config
            )

    except Exception as e:
        # In the case of any errors parsing the YAML configurations, Log the errors, but
        # continue to parse the other YAML configurations
        logging.error(e)
        logging.error(traceback.print_exc())
        logging.error("Error while parsing YAML Configuration: {}".format(config_file))
