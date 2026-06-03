# adapted from out submit_dap_run endpoint and the airflow example for batch
# https://github.com/apache/airflow/blob/providers-amazon/9.25.0/providers/amazon/tests/system/amazon/aws/example_batch.py
#
# This has not been tested in any way as of 2026.04.27. We need to get our job
# def back to working, but this is a start of what could become the bactopia and
# kraken2 workflow. This is using what airflow refers to as taskflow.
#
# Assumes:
# * we have the aws airflow provider installed
# * we have created the batch environment and job queue already
# * we're using airflow 3.1+

# TODO:
# - trace all the imports and constants to make sure they're needed. much of
#   this was from the example and may be removable now

import io
import logging
from datetime import datetime

import boto3
from airflow.providers.amazon.aws.operators.batch import (
    BatchCreateComputeEnvironmentOperator,
    BatchOperator,
)
from airflow.providers.amazon.aws.operators.ecs import (
    EcsDeregisterTaskDefinitionOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.batch import (
    BatchComputeEnvironmentSensor,
    BatchJobQueueSensor,
    BatchSensor,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import chain, dag, task
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "bactopia_kraken2_v3_2_0"

DAG_DISPLAY_NAME = "Bactopia v3.2.0 and Kraken2 (through Bactopia) v3.2.0"
DAG_DESCRIPTION = (
    "Workflow that will run bactopia v3.2.0 followed by kraken2 (as a bactopia "
    "tool) using bactopia v3.2.0. Both are run in AWS Batch."
)

BACTOPIA_PROJ = "bactopia/bactopia"
BACTOPIA_VERSION = "v3.2.0"
KRAKEN2_PROJ = "bactopia/bactopia"
KRAKEN2_VERSION = "v3.2.0"

# TODO: these need to be provided and not hard coded
WORKFLOW_QUEUE_NAME = "ccd-pvsl-workflows-btch-jobq-e326d2f"
NEXTFLOW_JOB_DEFINITION = "ccd-pvsl-nextflow-jobdef"
JOB_QUEUE_NAME = "ccd-pvsl-analysis-btch-jobq-0a107a5"


# TODO: need to figure out how we'll be structuring the config dict for tasks.
#       it's going to the json body of the post we send from the API the front
#       end hits, and will be specific to a workflow. We'll probably want
#       something like:
# {
#   "expected_dap_name": {
#       "expected_parm1": value,
#       ...
#    },
#   "another_expected_dap_name": {
#       "expected_parm1": value,
#       ...
#    }
# }
#
# The front end will be grabbing values from the user based on json schema
# sent from the back end. So if we need params from the frontend for things
# other than DAPs (e.g. if we wanted to allow them to set the name for a
# report generated from a task) we'd need a mechanism to tell the FE about that
# schema as well. It may be worth considering a workflow param schema that ties
# the room together since we need some way of telling the front end "whn you
# asked for a bactopia with kraken workflow, that resulted in the need for the
# following parameters". Also need to figure out how this plays with versioning
# of DAPs. wouldn't necessarily want to have a workflow for every combination of
# versions of all DAPs
# Maybe we need to add a pipeline-id to the profile. project name is no bueno
# cause its the same for bactopia and kraken. pipeline name is not great either
# cause it's for people but feels like there should be something for both
# "Bactopia ONT Sample" pipelines that we can use to key config on in the tasks
# here. Versioning may play a role too, but if one workflow starts getting used
# for a ton of versions and the versions start having divergent config, then the
# workflow becomes hard to maintain


@dag(
    dag_id=DAG_ID,
    dag_display_name=DAG_DISPLAY_NAME,
    description=DAG_DESCRIPTION,
    # this DAG must be triggered, it is not on a schedule
    schedule="@once",
    start_date=datetime.now(),
    # if another run of this is already scheduled, do not supercede. If this was
    # True the latest run would be the one run
    catchup=False,
)
def bactopia_and_kraken2_v3_2_0():

    submit_bactopia_job = BatchOperator(
        task_id="submit_bactopia_batch_job",
        job_name=("{{ dag_run.dag_id }}-bactopia-to-kraken2-wf-bactopia-job"),
        job_queue=WORKFLOW_QUEUE_NAME,
        job_definition=NEXTFLOW_JOB_DEFINITION,
        container_overrides={
            "environment": [
                {"name": "PIPELINE", "value": f"{BACTOPIA_PROJ}"},
                {"name": "PIPELINE_VERSION", "value": f"{BACTOPIA_VERSION}"},
                {"name": "PIPELINE_QUEUE", "value": f"{JOB_QUEUE_NAME}"},
                {
                    "name": "NF_OPTS",
                    "value": (
                        "--outdir  {{ params.bactopia.pipelineOutputBucket }}/{{ params.bactopia.pipelineOutputPrefix }} "
                        "--sample {{ params.bactopia['--sample'] }} "
                        "{{ params.bactopia.nextflowOptions }}"
                    ),
                },
            ]
        },
    )

    create_k2_include = S3CreateObjectOperator(
        task_id="create_k2_include",
        s3_bucket="{{ params.kraken2.pipelineOutputBucketName }}",
        s3_key="batch_job_scratch/kraken2/{{ dag_run.dag_id }}-k2-include.txt",
        data="{{ params.bactopia['--sample'] }}",
        replace=True,
    )

    wait_for_k2_include = S3KeySensor(
        task_id="wait_for_kraken_2_include_file",
        bucket_name="{{ params.kraken2.pipelineOutputBucketName }}",
        bucket_key="batch_job_scratch/kraken2/{{ dag_run.dag_id }}-k2-include.txt",
    )

    wait_for_k2_include.poke_interval = 10

    submit_kraken2_job = BatchOperator(
        task_id="submit_kraken2_batch_job",
        job_name="{{ dag_run.dag_id }}-bactopia-to-kraken2-wf-kraken2-job",
        job_queue=WORKFLOW_QUEUE_NAME,
        job_definition=NEXTFLOW_JOB_DEFINITION,
        container_overrides={
            "environment": [
                {"name": "PIPELINE", "value": f"{KRAKEN2_PROJ}"},
                {"name": "PIPELINE_VERSION", "value": f"{KRAKEN2_VERSION}"},
                {"name": "PIPELINE_QUEUE", "value": f"{JOB_QUEUE_NAME}"},
                {
                    "name": "NF_OPTS",
                    "value": (
                        "--bactopia {{ params.bactopia.pipelineOutputBucket }}/"
                        "{{ params.bactopia.pipelineOutputPrefix }} "
                        f"--aws_queue {JOB_QUEUE_NAME} "
                        "--wf kraken2 {{ params.kraken2.nextflowOptions }} "
                        "--include {{ params.bactopia.pipelineOutputBucket }}/batch_job_scratch/kraken2/{{ dag_run.dag_id }}-k2-include.txt "
                        "--kraken2_db /mnt/nextflow_shared_data/kraken2 "
                        "--aws_volumes /opt/conda:/mnt/conda,/mnt/nextflow_shared_data:/mnt/nextflow_shared_data:ro"
                    ),
                },
            ]
        },
    )

    # TODO: add report generation here

    chain(
        submit_bactopia_job, create_k2_include, wait_for_k2_include, submit_kraken2_job
    )


bactopia_and_kraken2_v3_2_0()
