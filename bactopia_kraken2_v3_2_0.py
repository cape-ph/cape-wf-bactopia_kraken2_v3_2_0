"""
CAPE Workflow: Bactopia v3.2.0 + Kraken2 Taxonomic Classification.

This Airflow DAG implements a two-stage bacterial genome analysis workflow:
1. Bactopia v3.2.0 - Genome assembly, QC, and annotation
2. Kraken2 (via Bactopia tools) - Taxonomic classification

Both pipelines run as Nextflow workflows in AWS Batch.

Architecture:
    - Validation task validates pipeline configs and converts to CLI strings
    - Bactopia BatchOperator runs genome assembly pipeline
    - S3 operators create and wait for kraken2 include file
    - Kraken2 BatchOperator runs taxonomic classification on assembled genomes

Configuration:
    Triggered via Airflow API with dag_run.conf structure:
    {
        "pipelineConfigs": [
            {
                "pipelineId": "bactopia-ont-v3.2.0" | "bactopia-illumina-v3.2.0",
                "nextflowOptions": {
                    "--sample": "sample-name",
                    "--outdir": "s3://bucket/path",
                    ...
                }
            },
            {
                "pipelineId": "bactopia-kraken2-v3.2.0",
                "nextflowOptions": {
                    "--wf": "kraken2",
                    "--bactopia": "s3://bucket/path",
                    "--kraken2_db": "/mnt/nextflow_shared_data/kraken2",
                    ...
                }
            }
        ]
    }

Requirements:
    - Airflow 3.0.6 with Amazon provider
    - AWS Batch environment with workflow and analysis queues
    - S3 buckets for input/output
    - Kraken2 database mounted at /mnt/nextflow_shared_data/kraken2

See meta.json for supported pipeline IDs and README.md for detailed usage.
"""

import json
import logging
import time
from datetime import datetime

import boto3
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import chain, dag, task
from botocore.exceptions import ClientError

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

# Report generation configuration.
#
# After the bactopia/kraken2 Batch jobs finish, their results land in the
# seqauto `result-raw` bucket and are transformed into `result-clean` by
# asynchronous Glue ETL jobs, then catalogued by the result-clean Glue crawler
# so Athena can query them. The report is produced by invoking the deployed
# canned-report lambda (`getcannedreport`), which queries Athena for the sample
# and renders the HTML template. We drive readiness with a crawl-then-probe
# loop: crawl, invoke the report lambda, and if the sample's data is not yet
# queryable (non-200), sleep and re-crawl, because newly ETL'd clean data only
# becomes queryable after a crawl.
#
# The report data function reads the sample metadata from the seqauto
# input-clean catalog (`input_meta`) and the bactopia results from the
# result-clean catalog, so both crawlers must run and be caught up before the
# report is queryable. We crawl input-clean and result-clean each attempt.
#
# TODO: these need to be provided and not hard coded (crawler names are
#       Pulumi-generated physical names and will change if the crawlers are
#       recreated; source them from the CrawlerAttrs DynamoDB table via the
#       getbucketcrawler API or a stable name later).
AWS_REGION = "us-east-2"
# TODO: source from the CrawlerAttrs DynamoDB table / getbucketcrawler API
#       rather than hard coding this Pulumi-generated physical name.
INPUT_CLEAN_CRAWLER_NAME = (
    "ccd-dlh-T-seqauto-input-clean-vbkt-crwl-gcrwl-be77632"
)
RESULT_CLEAN_CRAWLER_NAME = (
    "ccd-dlh-T-seqauto-result-clean-vbkt-crwl-gcrwl-1ceb6f5"
)
# Crawlers to run/wait on each attempt, in order. input-clean first so sample
# metadata is catalogued before the result tables that reference it.
SEQAUTO_CRAWLER_NAMES = (
    INPUT_CLEAN_CRAWLER_NAME,
    RESULT_CLEAN_CRAWLER_NAME,
)
REPORT_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397883306:function:"
    "ccd-pvsl-capi-api-getcannedreport-lmbdfn-b295d26"
)
REPORT_ID = "bactopia-single-sample-analysis"

# The report is written to the seqauto artifacts bucket at
# s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/<sample>/{REPORT_OUTPUT_FILENAME}
# TODO: bucket name is a Pulumi-generated physical name; source it from stack
#       config/exports rather than hard coding it.
REPORT_OUTPUT_BUCKET = "ccd-dlh-t-seqauto-artifacts-vbkt-s3-d2421eb"
REPORT_OUTPUT_PREFIX = "reports"
REPORT_OUTPUT_FILENAME = "bactopia.html"

# Crawl-then-probe loop tuning.
REPORT_MAX_ATTEMPTS = 30
REPORT_ATTEMPT_SLEEP_SECONDS = 60
CRAWLER_POLL_INTERVAL_SECONDS = 15
CRAWLER_WAIT_TIMEOUT_SECONDS = 900

# NOTE: This is setting up for future. Right now this DAG only supports
#       bactopia 3.2.0 and kraken through bactopia 3.2.0. The ids for those
#       pipelines are in this dict. Additionally we have a notion of required
#       parameters in here as well.
# TODO: The required parameters should instead come from our
#       `/workflows/pipelineprofiles` endpoint, which gives the schema for a
#       profile. This endpoint also gives the pipelineId, project and version
#       for each DAP configured for the workflow. using some combo of these
#       things along with the below constant, we should be able to get rid of
#       BACTOPIA_PROJ, BACTOPIA_VERSION, KRAKEN2_PROJ, and KRAKEN2_VERSION
#       constants. this will be done in issue #10.
EXPECTED_PIPELINES = {
    "bactopia": {
        "pipeline_ids": ["bactopia-ont-v3.2.0"],
        "required_fields": ["--sample", "--outdir"],
    },
    "kraken2": {
        "pipeline_ids": ["bactopia-kraken2-v3.2.0"],
        "required_fields": [],
    },
}

K2_INCLUDE_PREFIX = "batch_job_scratch/kraken2/"
K2_INCLUDE_SUFFIX = "-k2-include.txt"


# Configuration format is now an object with pipelineConfigs passed via dag_run.conf:
#
# Example config:
# {
#   "pipelineConfigs": [
#     {
#       "pipelineId": "bactopia-ont-v3.2.0",
#       "nextflowOptions": {
#         "--sample": "sample-001",
#         "--outdir": "s3://my-bucket/bactopia-output",
#         "--min_genome_size": "2000000"
#       }
#     },
#     {
#       "pipelineId": "bactopia-kraken2-v3.2.0",
#       "nextflowOptions": {
#         "--some-option": "value"
#       }
#     }
#   ]
# }
#
# The validate_and_extract_nextflow_configs task handles:
# - Validation that expected pipelineIds are present
# - Validation that required fields exist in nextflowOptions
# - Conversion of nextflowOptions dict to CLI string
# - Making configs available to downstream tasks via XCom
#
# See EXPECTED_PIPELINES constant for supported pipelineIds and required fields.


# NOTE: functions and tasks outside the DAG are intended to eventually be in a
#       reusable library as they can be used by many DAGs


def nextflow_options_to_cli_string(options_dict: dict) -> str:
    """
    Convert nextflow options dict to CLI string.

    Args:
        options_dict: Dictionary of nextflow options, e.g.:
                     {"--sample": "s001", "--outdir": "s3://bucket/path"}

    Returns:
        CLI string with options in dict insertion order, e.g.:
        "--sample s001 --outdir s3://bucket/path"
    """
    parts = []
    for key, value in options_dict.items():
        parts.append(f"{key} {value}")
    return " ".join(parts)


def extract_s3_bucket_name(s3_path: str) -> str:
    """
    Extract bucket name from S3 path (lenient validation).

    Args:
        s3_path: S3 URL like "s3://bucket-name/prefix/path" or "s3://bucket-name"

    Returns:
        Bucket name extracted from the path, e.g., "bucket-name"

    Raises:
        ValueError: If path doesn't contain "s3://" or has no bucket name

    Examples:
        "s3://my-bucket/path/to/data" -> "my-bucket"
        "s3://bucket-name" -> "bucket-name"

    TODO: Eventually want bucket name in config directly,
    but need to handle non-nextflow pipelines first.
    """
    if "s3://" not in s3_path:
        raise ValueError(f"Invalid S3 path (missing s3://): {s3_path}")

    path_without_scheme = s3_path.split("s3://", 1)[1]

    if not path_without_scheme:
        raise ValueError(f"Invalid S3 path (no bucket name): {s3_path}")

    bucket_name = path_without_scheme.split("/")[0]

    if not bucket_name:
        raise ValueError(f"Invalid S3 path (empty bucket name): {s3_path}")

    return bucket_name


def run_crawler_and_wait(glue_client, crawler_name: str) -> None:
    """
    Start the named Glue crawler (if not already running) and wait for it to
    return to the READY state.

    If the crawler is already running (for example the scheduled daily crawl),
    we skip starting it and just wait for the in-flight run to finish.

    Args:
        glue_client: A boto3 Glue client.
        crawler_name: The physical name of the crawler to run.

    Raises:
        TimeoutError: If the crawler does not return to READY within
                      CRAWLER_WAIT_TIMEOUT_SECONDS.
    """
    try:
        glue_client.start_crawler(Name=crawler_name)
        log.info("Started Glue crawler '%s'", crawler_name)
    except ClientError as err:
        if err.response["Error"]["Code"] != "CrawlerRunningException":
            raise
        log.info(
            "Glue crawler '%s' already running; waiting for it to finish",
            crawler_name,
        )

    deadline = time.monotonic() + CRAWLER_WAIT_TIMEOUT_SECONDS
    # give the crawler a moment to leave READY before we start polling so we
    # don't observe the pre-start READY state and return immediately
    time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)
    while True:
        state = glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "READY":
            log.info("Glue crawler '%s' finished (state READY)", crawler_name)
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Glue crawler '{crawler_name}' did not finish within "
                f"{CRAWLER_WAIT_TIMEOUT_SECONDS}s (last state {state})."
            )
        time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)


def invoke_report_lambda(
    lambda_client, sample_id: str
) -> tuple[int, str | None]:
    """
    Invoke the canned-report lambda for a sample and return (status_code, body).

    The report lambda returns HTTP 200 with the rendered HTML once the sample's
    data is catalogued in Athena. While the (asynchronous) ETL has not yet
    produced queryable data the data function raises and the handler returns a
    non-200 status. A FunctionError (an unhandled lambda crash) is also treated
    as not-ready.

    Args:
        lambda_client: A boto3 Lambda client.
        sample_id: The bactopia `--sample` value to report on.

    Returns:
        A tuple of (status_code, body). body is the report HTML on success and
        None otherwise.
    """
    event = {
        "queryStringParameters": {
            "reportId": REPORT_ID,
            "sampleId": sample_id,
            "format": "html",
        }
    }
    response = lambda_client.invoke(
        FunctionName=REPORT_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )

    if response.get("FunctionError"):
        error_payload = response["Payload"].read().decode("utf-8")
        log.warning("Report lambda returned FunctionError: %s", error_payload)
        return 500, None

    try:
        payload = json.loads(response["Payload"].read())
    except (ValueError, KeyError) as err:
        log.warning("Could not parse report lambda response: %s", err)
        return 500, None
    status_code = payload.get("statusCode", 500)
    if status_code != 200:
        log.warning(
            "Report lambda handler returned status %s: %s",
            status_code,
            payload.get("body"),
        )
        return status_code, None

    return 200, payload.get("body")


@task
def generate_and_store_report(**context) -> str:
    """
    Generate the bactopia single-sample analysis report and store it in S3.

    Runs a bounded crawl-then-probe loop: trigger the seqauto input-clean and
    result-clean Glue crawlers, wait for them to finish, then invoke the
    deployed canned-report lambda for the bactopia `--sample`. On a 200 the
    returned HTML is written to
    s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/{sample_id}/{REPORT_OUTPUT_FILENAME}.
    Otherwise the sample is not yet queryable, so we sleep and re-crawl (newly
    ETL'd clean data only becomes queryable after a crawl).

    Args:
        context: Airflow context (automatically passed by TaskFlow API). The
                 sample id is read from the validated configs in XCom:
                 configs['bactopia']['nextflowOptions']['--sample'].

    Returns:
        The S3 URI the report HTML was written to.

    Raises:
        RuntimeError: If the report is not ready within REPORT_MAX_ATTEMPTS.

    TODO: crawler names, lambda ARN, and output location are hard coded and
          must be parameterized (see module constants).
    """
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    sample_id = configs["bactopia"]["nextflowOptions"]["--sample"]

    glue_client = boto3.client("glue", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    report_html = None
    for attempt in range(1, REPORT_MAX_ATTEMPTS + 1):
        log.info(
            "Report attempt %s/%s for sample '%s'",
            attempt,
            REPORT_MAX_ATTEMPTS,
            sample_id,
        )
        for crawler_name in SEQAUTO_CRAWLER_NAMES:
            run_crawler_and_wait(glue_client, crawler_name)

        status_code, body = invoke_report_lambda(lambda_client, sample_id)
        if status_code == 200 and body:
            log.info("Report ready for sample '%s'", sample_id)
            report_html = body
            break

        if attempt < REPORT_MAX_ATTEMPTS:
            log.info(
                "Report not ready (status=%s) for sample '%s'; sleeping %ss "
                "before re-crawl",
                status_code,
                sample_id,
                REPORT_ATTEMPT_SLEEP_SECONDS,
            )
            time.sleep(REPORT_ATTEMPT_SLEEP_SECONDS)

    if report_html is None:
        raise RuntimeError(
            f"Report for sample '{sample_id}' was not ready after "
            f"{REPORT_MAX_ATTEMPTS} attempts."
        )

    s3_key = f"{REPORT_OUTPUT_PREFIX}/{sample_id}/{REPORT_OUTPUT_FILENAME}"
    s3_client.put_object(
        Bucket=REPORT_OUTPUT_BUCKET,
        Key=s3_key,
        Body=report_html.encode("utf-8"),
        ContentType="text/html",
    )
    s3_uri = f"s3://{REPORT_OUTPUT_BUCKET}/{s3_key}"
    log.info("Wrote report for sample '%s' to %s", sample_id, s3_uri)
    return s3_uri


# TODO: in order for this to be reusable, we'll need to pass in whatever form
#       `EXPECTED_PIPELINES` takes on. right now it assumes it's in scope and
#       just uses it.
@task
def validate_and_extract_nextflow_configs(
    fail_on_any_error: bool = True, **context
) -> dict:
    """
    Validates pipeline configs from dag_run.conf and extracts/transforms them.

    This task ensures all expected pipelines are present with valid configuration,
    and converts nextflowOptions to CLI strings for use in BatchOperators.

    Args:
        fail_on_any_error: If True, fail if any expected pipeline is
                          missing/invalid (default: True). Added for future
                          library reusability.
        context: Airflow context (automatically passed by TaskFlow API).
                 Config is accessed from context['dag_run'].conf with structure:
                 {
                     "pipelineConfigs": [
                         {
                             "pipelineId": str (must match one from EXPECTED_PIPELINES),
                             "nextflowOptions": dict (must contain required_fields)
                         },
                         ...
                     ]
                 }

    Returns:
        Dict with keys from EXPECTED_PIPELINES, each containing:
        {
            "bactopia": {
                "pipelineId": "bactopia-ont-v3.2.0",
                "nextflowOptions": {"--sample": "s001", "--outdir": "s3://..."},
                "nextflowOptionsCli": "--sample s001 --outdir s3://..."
            },
            "kraken2": {
                "pipelineId": "bactopia-kraken2-v3.2.0",
                "nextflowOptions": {...},
                "nextflowOptionsCli": "..."
            }
        }

    Raises:
        ValueError: If expected pipelineId not found or required fields missing
                   (when fail_on_any_error is True). Error messages:
                   - Missing config: "No config found for pipeline 'X'. Expected
                     one of: [list]. Received pipelineIds: [actual]"
                   - Missing field: "Missing required field '--sample' for
                     pipeline 'bactopia'. Required fields: [list]"

    TODO: Investigate XCom size limits for large nextflowOptions configs.
    """
    result = {}

    conf = context["dag_run"].conf

    if conf is None:
        raise ValueError(
            "No configuration provided. DAG must be triggered with 'conf' parameter."
        )

    if "pipelineConfigs" not in conf:
        raise ValueError(
            "Invalid config structure: missing 'pipelineConfigs' key. "
            "Expected config format: {'pipelineConfigs': [...]}"
        )

    pipeline_configs = conf["pipelineConfigs"]
    received_pipeline_ids = [
        item.get("pipelineId") for item in pipeline_configs
    ]

    for pipeline_key, pipeline_spec in EXPECTED_PIPELINES.items():
        expected_ids = pipeline_spec["pipeline_ids"]
        required_fields = pipeline_spec["required_fields"]

        matching_config = None
        for item in pipeline_configs:
            if item.get("pipelineId") in expected_ids:
                matching_config = item
                break

        if matching_config is None:
            error_msg = (
                f"No config found for pipeline '{pipeline_key}'. "
                f"Expected one of: {expected_ids}. "
                f"Received pipelineIds: {received_pipeline_ids}"
            )
            if fail_on_any_error:
                raise ValueError(error_msg)
            else:
                log.warning(error_msg)
                continue

        nextflow_options = matching_config.get("nextflowOptions", {})
        for field in required_fields:
            if field not in nextflow_options:
                error_msg = (
                    f"Missing required field '{field}' for pipeline '{pipeline_key}'. "
                    f"Required fields: {required_fields}"
                )
                if fail_on_any_error:
                    raise ValueError(error_msg)
                else:
                    log.warning(error_msg)

        nextflow_cli = nextflow_options_to_cli_string(nextflow_options)

        result[pipeline_key] = {
            "pipelineId": matching_config["pipelineId"],
            "nextflowOptions": nextflow_options,
            "nextflowOptionsCli": nextflow_cli,
        }

    return result


@dag(
    dag_id=DAG_ID,
    dag_display_name=DAG_DISPLAY_NAME,
    description=DAG_DESCRIPTION,
    schedule="@once",
    start_date=datetime.now(),
    catchup=False,
    user_defined_filters={"extract_s3_bucket": extract_s3_bucket_name},
)
def bactopia_and_kraken2_v3_2_0():

    configs = validate_and_extract_nextflow_configs()

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
                    "value": "{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptionsCli'] }}",
                },
            ]
        },
    )

    create_k2_include = S3CreateObjectOperator(
        task_id="create_k2_include",
        s3_bucket="{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptions']['--outdir'] | extract_s3_bucket }}",
        s3_key=f"{K2_INCLUDE_PREFIX}{{{{ dag_run.dag_id }}}}{K2_INCLUDE_SUFFIX}",
        data="{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptions']['--sample'] }}",
        replace=True,
    )

    wait_for_k2_include = S3KeySensor(
        task_id="wait_for_kraken_2_include_file",
        bucket_name="{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptions']['--outdir'] | extract_s3_bucket }}",
        bucket_key=f"{K2_INCLUDE_PREFIX}{{{{ dag_run.dag_id }}}}{K2_INCLUDE_SUFFIX}",
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
                        f"--aws_queue {JOB_QUEUE_NAME} "
                        "{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['kraken2']['nextflowOptionsCli'] }} "
                        f"--include s3://{{{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptions']['--outdir'] | extract_s3_bucket }}}}/{K2_INCLUDE_PREFIX}{{{{ dag_run.dag_id }}}}{K2_INCLUDE_SUFFIX} "
                        "--aws_volumes /opt/conda:/mnt/conda,/mnt/nextflow_shared_data:/mnt/nextflow_shared_data:ro"
                    ),
                },
            ]
        },
    )

    # Report generation. Once the kraken2 job finishes, the bactopia/kraken2
    # results have been written to the seqauto result-raw bucket and are being
    # transformed into result-clean by asynchronous Glue ETL jobs. This task
    # runs a crawl-then-probe loop against the input-clean and result-clean
    # crawlers and the canned-report lambda, then writes the rendered HTML to
    # S3. See generate_and_store_report and the report constants for details.
    #
    # NOTE: this stores the report at a fixed S3 location and does not carry any
    #       authn context; the download-time authz concerns of the existing API
    #       endpoint are intentionally out of scope for this workflow step.
    generate_report = generate_and_store_report()

    chain(
        configs,
        submit_bactopia_job,
        create_k2_include,
        wait_for_k2_include,
        submit_kraken2_job,
        generate_report,
    )


bactopia_and_kraken2_v3_2_0()
