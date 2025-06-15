import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

# Prerequisites:
#   1. Create Opaque secret "scraping-demo-secret" with required keys
#   2. Create Docker registry secret to pull from private registry, e.g. using
#      kubectl create secret docker-registry docker-registry-creds --docker-server=X --docker-username=X --docker-password=X --namespace airflow-1

K8S_NAMESPACE = 'airflow-1'

ETL_IMAGE = 'datainfrapilotacr.azurecr.io/scraping-demo:0.2'

SECRET_NAME = 'scraping-demo-secret'

kpo_shared_args = {
    'namespace': K8S_NAMESPACE,
    'image': ETL_IMAGE,
    'image_pull_secrets': [k8s.V1LocalObjectReference('docker-registry-creds')],
    'get_logs': True,
    'do_xcom_push': False,
    'is_delete_operator_pod': True,
    'startup_timeout_seconds': 600,
}

common_env_vars = [
    k8s.V1EnvVar(
        name='AZURE_CLIENT_ID',
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='azure_client_id')),
    ),
    k8s.V1EnvVar(
        name='AZURE_TENANT_ID',
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='azure_tenant_id')),
    ),
    k8s.V1EnvVar(
        name='AZURE_CLIENT_SECRET',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='azure_client_secret')
        ),
    ),
    k8s.V1EnvVar(name='ADLS_ACCOUNT_NAME', value='datainfrapilotdemosa'),
    k8s.V1EnvVar(name='ADLS_CONTAINER_NAME', value='data'),
    k8s.V1EnvVar(name='POSTGRES_DB_NAME', value='real_estate'),
    k8s.V1EnvVar(
        name='POSTGRES_USER',
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='postgres_user')),
    ),
    k8s.V1EnvVar(
        name='POSTGRES_PASSWORD',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='postgres_password')
        ),
    ),
    k8s.V1EnvVar(
        name='POSTGRES_HOST',
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='postgres_host')),
    ),
    k8s.V1EnvVar(
        name='SCRAPERAPI_KEY',
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=SECRET_NAME, key='scraperapi_key')),
    ),
]


def prepare_project_listing_type_task_group(project: str, listing_type: str) -> TaskGroup:
    with TaskGroup(group_id=f'{project}_{listing_type}_group') as task_group:
        scrape = KubernetesPodOperator(
            task_id=f'{project}_scrape_{listing_type}',
            name=f'{project}-scrape-{listing_type}',
            cmds=['python3'],
            arguments=[
                '-m',
                'src.orchestration.scrape',
                '--project',
                project,
                '--listing-type',
                listing_type,
                '--batch-id',
                '{{ tomorrow_ds_nodash }}',
            ],
            env_vars=common_env_vars,
            **kpo_shared_args,
        )

        transform = KubernetesPodOperator(
            task_id=f'{project}_transform_{listing_type}',
            name=f'{project}-transform-{listing_type}',
            cmds=['python3'],
            arguments=[
                '-m',
                'src.orchestration.transform',
                '--project',
                project,
                '--listing-type',
                listing_type,
                '--batch-id',
                '{{ tomorrow_ds_nodash }}',
            ],
            env_vars=common_env_vars,
            **kpo_shared_args,
        )

        upload = KubernetesPodOperator(
            task_id=f'{project}_upload_{listing_type}',
            name=f'{project}-upload-{listing_type}',
            cmds=['python3'],
            arguments=[
                '-m',
                'src.orchestration.upload_to_db',
                '--project',
                project,
                '--listing-type',
                listing_type,
                '--batch-id',
                '{{ tomorrow_ds_nodash }}',
            ],
            env_vars=common_env_vars,
            **kpo_shared_args,
        )

        scrape >> transform >> upload

    return task_group


with DAG(
    dag_id='real_estate_pipeline_demo',
    dag_display_name='Real Estate Scraping Pipeline',
    start_date=datetime.datetime(2025, 6, 7),
    schedule='@daily',
    catchup=False,
    tags=['real_estate', 'demo'],
    doc_md="""
    ### Real Estate ETL Pipeline

    This DAG orchestrates the scraping, transformation, and uploading of real estate
    data from two sources: sreality.cz and bezrealitky.cz.

    The pipeline covers 2 types of listings - rent and sale. Each listing type undergoing a
    scrape -> transform -> upload sequence. Data is staged in Azure Data Lake Storage Gen2 (ADLS)
    between scrape and transform, and final data is uploaded to PostgreSQL.
    """,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(seconds=300),
        'execution_timeout': datetime.timedelta(hours=1),
    },
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    sreality_rent_tg = prepare_project_listing_type_task_group('sreality', 'rent')
    sreality_sale_tg = prepare_project_listing_type_task_group('sreality', 'sale')

    bezrealitky_rent_tg = prepare_project_listing_type_task_group('bezrealitky', 'rent')
    bezrealitky_sale_tg = prepare_project_listing_type_task_group('bezrealitky', 'sale')

    start_task >> [sreality_rent_tg, sreality_sale_tg, bezrealitky_rent_tg, bezrealitky_sale_tg] >> end_task
