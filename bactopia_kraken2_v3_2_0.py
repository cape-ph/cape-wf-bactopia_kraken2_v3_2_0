"""
CAPE Workflow: Bactopia v3.2.0 + Kraken2 Taxonomic Classification.

This Airflow DAG implements a two-stage bacterial genome analysis workflow:
1. Bactopia v3.2.0 - Genome assembly, QC, and annotation
2. Kraken2 (via Bactopia tools) - Taxonomic classification

Both pipelines run as Nextflow workflows in AWS Batch.

Architecture:
    - Validation task validates pipeline configs and converts to CLI strings
    - Bactopia BatchOperator submits the genome assembly pipeline without
      blocking (wait_for_completion=False)
    - An S3KeySensor gates kraken2 on the bactopia QC output for the sample so
      the two pipelines run in parallel: once QC has written
      <outdir>/<sample>/main/qc/<sample>.fastq.gz the Kraken2
      BatchOperator runs while the rest of bactopia is still executing
    - S3 operators create and wait for the kraken2 include file
    - A BatchSensor waits for the bactopia job to finish, then the bactopia
      report is generated from the full bactopia results
    - The bactopia and kraken2 report branches are independent: each report is
      generated as soon as its own inputs are ready, and a failure in one
      branch does not block the other (the DAG has no fail-fast). The run is
      done once both reports are generated or a branch has failed
    - After kraken2 finishes, a self-contained HTML taxonomic report is built
      directly from the kraken2 tool report file and written to the artifacts
      bucket (see generate_and_store_kraken2_report); it fails silently if the
      report file is missing or empty (no classifications)

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

import html
import json
import logging
import time
from datetime import datetime

import boto3
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.batch import BatchSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import chain, dag, task
from botocore.exceptions import ClientError
from jinja2 import Template

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
# Crawlers to run/wait on each attempt. They run in parallel (started
# together, then awaited together); order is irrelevant.
SEQAUTO_CRAWLER_NAMES = (
    INPUT_CLEAN_CRAWLER_NAME,
    RESULT_CLEAN_CRAWLER_NAME,
)
REPORT_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397883306:function:"
    "ccd-pvsl-capi-api-getcannedreport-lmbdfn-b295d26"
)
REPORT_ID = "bactopia-single-sample-analysis"

# Kraken2 taxonomic-classification report. Unlike the bactopia report, this is
# built directly from the kraken2 tool's own report file rather than the
# canned-report lambda: after the kraken2 job finishes it reads
# <outdir>/<sample>/tools/kraken2/<sample>.kraken2.report.txt from S3, renders a
# self-contained HTML tree, and writes it next to the bactopia report. This is
# a deliberate stopgap expected to be replaced by an official cape-cod report
# type (ETL + Glue/Athena + data handler + reports/create); see
# cape-ph/cape-wf-bactopia_kraken2_v3_2_0#16, the KRAKEN2 REPORT section below,
# and generate_and_store_kraken2_report.
#
# Path (relative to the bactopia --outdir) of the kraken2 tool report file.
KRAKEN2_REPORT_SOURCE_TEMPLATE = (
    "{sample}/tools/kraken2/{sample}.kraken2.report.txt"
)

# The report is written to the seqauto artifacts bucket at
# s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/<sample>/{REPORT_OUTPUT_FILENAME}
# TODO: bucket name is a Pulumi-generated physical name; source it from stack
#       config/exports rather than hard coding it.
REPORT_OUTPUT_BUCKET = "ccd-dlh-t-seqauto-artifacts-vbkt-s3-d2421eb"
REPORT_OUTPUT_PREFIX = "reports"
REPORT_OUTPUT_FILENAME = "bactopia.html"
# Kraken2 report object name, written under the same per-sample prefix as the
# bactopia report:
# s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/<sample>/kraken2.html
KRAKEN2_REPORT_OUTPUT_FILENAME = "kraken2.html"

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


def start_crawler_if_idle(glue_client, crawler_name: str) -> None:
    """
    Start the named Glue crawler unless it is already running.

    If the crawler is already running (for example the scheduled daily crawl or
    a concurrent DAG run), the start is skipped and the in-flight run is left to
    finish.

    Args:
        glue_client: A boto3 Glue client.
        crawler_name: The physical name of the crawler to start.
    """
    try:
        glue_client.start_crawler(Name=crawler_name)
        log.info("Started Glue crawler '%s'", crawler_name)
    except ClientError as err:
        if err.response["Error"]["Code"] != "CrawlerRunningException":
            raise
        log.info(
            "Glue crawler '%s' already running; will wait for it to finish",
            crawler_name,
        )


def run_crawlers_and_wait(glue_client, crawler_names) -> None:
    """
    Start the given Glue crawlers concurrently and wait for all of them to
    return to the READY state.

    The crawlers are all started first, then polled together against a single
    shared timeout, so they run in parallel rather than sequentially. Order is
    irrelevant to the caller.

    Args:
        glue_client: A boto3 Glue client.
        crawler_names: An iterable of crawler physical names to run.

    Raises:
        TimeoutError: If any crawler does not return to READY within
                      CRAWLER_WAIT_TIMEOUT_SECONDS.
    """
    for crawler_name in crawler_names:
        start_crawler_if_idle(glue_client, crawler_name)

    deadline = time.monotonic() + CRAWLER_WAIT_TIMEOUT_SECONDS
    # give the crawlers a moment to leave READY before we start polling so we
    # don't observe the pre-start READY state and return immediately
    time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)
    pending = list(crawler_names)
    while pending:
        still_running = []
        for crawler_name in pending:
            state = glue_client.get_crawler(Name=crawler_name)["Crawler"][
                "State"
            ]
            if state == "READY":
                log.info(
                    "Glue crawler '%s' finished (state READY)", crawler_name
                )
            else:
                still_running.append(crawler_name)

        if not still_running:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Glue crawlers {still_running} did not finish within "
                f"{CRAWLER_WAIT_TIMEOUT_SECONDS}s."
            )
        pending = still_running
        time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)


def invoke_report_lambda(
    lambda_client, sample_id: str, report_id: str = REPORT_ID
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
        sample_id: The `--sample` value to report on.
        report_id: The canned-report reportId to render (defaults to the
                   bactopia report).

    Returns:
        A tuple of (status_code, body). body is the report HTML on success and
        None otherwise.
    """
    event = {
        "queryStringParameters": {
            "reportId": report_id,
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


def _generate_and_store_report(
    sample_id: str, report_id: str, output_filename: str
) -> str:
    """
    Run the crawl-then-probe report loop for a sample and store the HTML in S3.

    Triggers the seqauto input-clean and result-clean Glue crawlers, waits for
    them, then invokes the canned-report lambda for report_id/sample_id. On a
    200 the returned HTML is written to
    s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/{sample_id}/{output_filename}.
    Otherwise the sample is not yet queryable, so it sleeps and re-crawls (newly
    ETL'd clean data only becomes queryable after a crawl).

    Args:
        sample_id: The sample to report on.
        report_id: The canned-report reportId to render.
        output_filename: The S3 object name to write under the sample prefix.

    Returns:
        The S3 URI the report HTML was written to.

    Raises:
        RuntimeError: If the report is not ready within REPORT_MAX_ATTEMPTS.

    TODO: crawler names, lambda ARN, and output location are hard coded and
          must be parameterized (see module constants).
    """
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    report_html = None
    for attempt in range(1, REPORT_MAX_ATTEMPTS + 1):
        log.info(
            "Report '%s' attempt %s/%s for sample '%s'",
            report_id,
            attempt,
            REPORT_MAX_ATTEMPTS,
            sample_id,
        )
        run_crawlers_and_wait(glue_client, SEQAUTO_CRAWLER_NAMES)

        status_code, body = invoke_report_lambda(
            lambda_client, sample_id, report_id
        )
        if status_code == 200 and body:
            log.info("Report '%s' ready for sample '%s'", report_id, sample_id)
            report_html = body
            break

        if attempt < REPORT_MAX_ATTEMPTS:
            log.info(
                "Report '%s' not ready (status=%s) for sample '%s'; sleeping "
                "%ss before re-crawl",
                report_id,
                status_code,
                sample_id,
                REPORT_ATTEMPT_SLEEP_SECONDS,
            )
            time.sleep(REPORT_ATTEMPT_SLEEP_SECONDS)

    if report_html is None:
        raise RuntimeError(
            f"Report '{report_id}' for sample '{sample_id}' was not ready "
            f"after {REPORT_MAX_ATTEMPTS} attempts."
        )

    s3_key = f"{REPORT_OUTPUT_PREFIX}/{sample_id}/{output_filename}"
    s3_client.put_object(
        Bucket=REPORT_OUTPUT_BUCKET,
        Key=s3_key,
        Body=report_html.encode("utf-8"),
        ContentType="text/html",
    )
    s3_uri = f"s3://{REPORT_OUTPUT_BUCKET}/{s3_key}"
    log.info(
        "Wrote report '%s' for sample '%s' to %s",
        report_id,
        sample_id,
        s3_uri,
    )
    return s3_uri


@task
def generate_and_store_report(**context) -> str:
    """
    Generate the bactopia single-sample analysis report and store it in S3.

    Thin wrapper over _generate_and_store_report that reads the bactopia
    `--sample` from the validated configs in XCom and renders REPORT_ID to
    REPORT_OUTPUT_FILENAME.

    Args:
        context: Airflow context (automatically passed by TaskFlow API). The
                 sample id is read from the validated configs in XCom:
                 configs['bactopia']['nextflowOptions']['--sample'].

    Returns:
        The S3 URI the report HTML was written to.

    Raises:
        RuntimeError: If the report is not ready within REPORT_MAX_ATTEMPTS.
    """
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    sample_id = configs["bactopia"]["nextflowOptions"]["--sample"]
    return _generate_and_store_report(
        sample_id, REPORT_ID, REPORT_OUTPUT_FILENAME
    )


# ============================================================================
# KRAKEN2 REPORT (direct-from-S3 HTML)
#
# Self-contained parser and HTML renderer for the kraken2 tool report file,
# plus the airflow task that reads it from S3 and stores the rendered report.
# This whole section is a deliberate stopgap, intended to be removed wholesale
# once an official cape-cod report type replaces it (migration tracked in
# cape-ph/cape-wf-bactopia_kraken2_v3_2_0#16).
# ============================================================================

RANK_LABELS = {
    "U": "Unclassified",
    "R": "Root",
    "D": "Domain",
    "K": "Kingdom",
    "P": "Phylum",
    "C": "Class",
    "O": "Order",
    "F": "Family",
    "G": "Genus",
    "S": "Species",
}

# Branches at or above this clade percentage start expanded in the "Relevant"
# view; everything else is collapsed so the report opens focused on the
# prevalent lineage.
DEFAULT_OPEN_PCT = 1.0
# Pixels of indentation applied to a taxon name per level of tree depth.
INDENT_PX = 16


def parse_kraken2_report(text: str) -> list[dict]:
    """Parse Kraken2 report text into a list of taxon rows.

    Each row carries the parsed numeric columns, the rank code and its base
    letter, the NCBI taxid, the cleaned scientific name, and the tree depth
    recovered from the name's leading-space indentation. Malformed lines (fewer
    than six tab-separated fields, or non-numeric counts) are skipped rather
    than failing, so a partial file still renders.
    """
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        pct, clade_reads, direct_reads, rank, taxid, name = parts[:6]
        try:
            row = {
                "pct": float(pct),
                "clade_reads": int(clade_reads),
                "direct_reads": int(direct_reads),
            }
        except ValueError:
            continue
        indent = len(name) - len(name.lstrip(" "))
        row.update(
            {
                "rank": rank.strip(),
                "rank_base": rank.strip()[:1],
                "taxid": taxid.strip(),
                "name": name.strip(),
                "depth": indent // 2,
            }
        )
        rows.append(row)
    return rows


def summarize(rows: list[dict]) -> dict:
    """Compute the report summary from parsed rows.

    Returns total/classified/unclassified read counts and percentages, the
    number of distinct taxa (excluding the unclassified and root pseudo-taxa),
    and the dominant species (rank code exactly "S", by clade reads).
    """
    unclassified = next((r for r in rows if r["rank"] == "U"), None)
    root = next((r for r in rows if r["rank"] == "R"), None)

    classified_reads = root["clade_reads"] if root else 0
    unclassified_reads = unclassified["clade_reads"] if unclassified else 0
    total_reads = classified_reads + unclassified_reads

    species = [r for r in rows if r["rank"] == "S"]
    top = max(species, key=lambda r: r["clade_reads"], default=None)

    def pct(part: int) -> float:
        return (100.0 * part / total_reads) if total_reads else 0.0

    return {
        "total_reads": total_reads,
        "classified_reads": classified_reads,
        "classified_pct": pct(classified_reads),
        "unclassified_reads": unclassified_reads,
        "unclassified_pct": pct(unclassified_reads),
        "distinct_taxa": sum(1 for r in rows if r["rank"] not in ("U", "R")),
        "top_species": top,
    }


def top_species(rows: list[dict], limit: int = 15) -> list[dict]:
    """Return the top species-level taxa (rank code exactly "S") by clade reads."""
    species = [r for r in rows if r["rank"] == "S"]
    species.sort(key=lambda r: r["clade_reads"], reverse=True)
    return species[:limit]


def build_tree(rows: list[dict]) -> list[dict]:
    """Nest the flat, depth-indented rows into a parent/child tree.

    Uses each row's depth (recovered from name indentation) with a running
    stack: a row hangs off the nearest preceding row of smaller depth. The
    unclassified and root pseudo-taxa sit at depth 0 as separate roots.
    """
    roots: list[dict] = []
    stack: list[dict] = []
    for row in rows:
        node = dict(row, children=[])
        while stack and stack[-1]["depth"] >= node["depth"]:
            stack.pop()
        if stack:
            stack[-1]["children"].append(node)
        else:
            roots.append(node)
        stack.append(node)
    return roots


def _default_open(node: dict) -> bool:
    """Whether a branch node starts expanded in the "Relevant" view.

    Species-level and below (rank base "S": species, serotypes, strains) stay
    collapsed to hide long variant lists; higher ranks expand when their clade
    share clears DEFAULT_OPEN_PCT, focusing the initial view on the prevalent
    lineage.
    """
    if node["rank_base"] == "S":
        return False
    return node["pct"] >= DEFAULT_OPEN_PCT


def _row_cells(node: dict, *, toggle: bool) -> str:
    """Render the six aligned cells (taxon, rank, metrics) for one tree row."""
    pad = node["depth"] * INDENT_PX
    caret_class = "caret" if toggle else "caret spacer"
    name = html.escape(node["name"])
    rank = html.escape(node["rank"])
    rank_label = html.escape(
        str(RANK_LABELS.get(node["rank_base"], node["rank"]))
    )
    taxid = html.escape(node["taxid"])
    return (
        f'<span class="c-name" style="padding-left:{pad}px">'
        f'<span class="{caret_class}"></span>'
        f'<span class="name">{name}</span></span>'
        f'<span><span class="rank-badge" title="{rank_label}">{rank}</span>'
        f"</span>"
        f'<span class="c-num">{node["pct"]:.2f}</span>'
        f'<span class="c-num">{node["clade_reads"]:,}</span>'
        f'<span class="c-num">{node["direct_reads"]:,}</span>'
        f'<span class="c-num">{taxid}</span>'
    )


def _render_node(node: dict) -> str:
    """Recursively render one tree node.

    Leaves render as plain rows; every node with children becomes its own
    collapsible <details>, so each level can be toggled independently. The
    default open/closed state (used by the "Relevant" view) comes from
    _default_open and is stamped onto the element as data-default.
    """
    children = node["children"]
    if not children:
        return f'<div class="row">{_row_cells(node, toggle=False)}</div>'
    open_attr = " open" if _default_open(node) else ""
    default_state = "open" if _default_open(node) else "closed"
    inner = "".join(_render_node(child) for child in children)
    return (
        f'<details class="node" data-default="{default_state}"{open_attr}>'
        f'<summary class="row">{_row_cells(node, toggle=True)}</summary>'
        f'<div class="children">{inner}</div></details>'
    )


def render_tree_html(rows: list[dict]) -> str:
    """Render the full collapsible taxonomic tree as an HTML fragment."""
    return "".join(_render_node(node) for node in build_tree(rows))


KRAKEN2_REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Kraken2 Taxonomic Classification</title>
    <style>
      html, body { height: auto; }
      body {
        margin: 0;
        padding: 24px;
        max-width: 980px;
        margin-inline: auto;
        font: 14px/1.6 system-ui, sans-serif;
        color: #0f172a;
        background: #fff;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
      }
      h1 {
        font-size: 1.75rem;
        margin-bottom: .25rem;
        color: oklch(0.3967 0.14 18.05);
      }
      .subtitle { color: #64748b; margin-top: 0; }
      h2 {
        font-size: 1.25rem;
        margin-top: 2rem;
        padding-bottom: .35rem;
        border-bottom: 1px solid #e2e8f0;
        color: oklch(0.3967 0.14 18.05);
      }
      .cards {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 16px;
      }
      .card {
        flex: 1 1 160px;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: .85rem 1rem;
        background: oklch(99.087% 0.00442 33.09);
      }
      .card .label {
        font-size: .75rem;
        text-transform: uppercase;
        letter-spacing: .04em;
        color: #64748b;
      }
      .card .value { font-size: 1.4rem; font-weight: 600; }
      .card .sub { font-size: .85rem; color: #64748b; }
      table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        margin-top: 12px;
        font-size: .95rem;
        background: #fff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
      }
      thead th {
        text-align: left;
        font-weight: 600;
        background: oklch(95.979% 0.00643 16.336);
        border-bottom: 1px solid #e2e8f0;
        padding: .6rem .75rem;
        white-space: nowrap;
      }
      tbody td {
        padding: .5rem .75rem;
        border-bottom: 1px solid oklch(97.478% 0.0064 16.315);
        vertical-align: middle;
      }
      tbody tr:last-child td { border-bottom: 0; }
      tbody tr:nth-child(odd) td {
        background: oklch(99.087% 0.00442 33.09);
      }
      td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
      .rank-badge {
        display: inline-block;
        min-width: 1.6rem;
        text-align: center;
        font-size: .72rem;
        font-weight: 600;
        color: #475569;
        background: #e2e8f0;
        border-radius: 6px;
        padding: .1rem .35rem;
      }
      .bar {
        position: relative;
        min-width: 90px;
        height: 1.05rem;
        background: oklch(97.478% 0.0064 16.315);
        border-radius: 5px;
        overflow: hidden;
      }
      .bar > span {
        position: absolute;
        inset: 0 auto 0 0;
        background: #3b82f6;
        opacity: .75;
      }
      .bar > em {
        position: relative;
        font-style: normal;
        font-size: .78rem;
        padding: 0 .4rem;
        color: #0f172a;
        line-height: 1.05rem;
      }
      .tree {
        --cols: minmax(0, 1fr) 3.5rem 4.5rem 6.5rem 6.5rem 5rem;
        margin-top: 12px;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
        font-size: .93rem;
      }
      .tree-head, .row {
        display: grid;
        grid-template-columns: var(--cols);
        align-items: center;
        column-gap: .5rem;
      }
      .tree-head {
        background: oklch(95.979% 0.00643 16.336);
        font-weight: 600;
        padding: .55rem .75rem;
        border-bottom: 1px solid #e2e8f0;
      }
      .row {
        padding: .32rem .75rem;
        border-bottom: 1px solid oklch(97.478% 0.0064 16.315);
      }
      .row:hover { background: oklch(99.087% 0.00442 33.09); }
      summary.row { list-style: none; cursor: pointer; }
      summary.row::-webkit-details-marker { display: none; }
      .c-name { display: flex; align-items: center; min-width: 0; }
      .c-name .name { word-break: break-word; }
      .c-num, .num { text-align: right; font-variant-numeric: tabular-nums; }
      .caret {
        flex: none;
        width: 0;
        height: 0;
        margin-right: .45rem;
        border-left: 5px solid #94a3b8;
        border-top: 4px solid transparent;
        border-bottom: 4px solid transparent;
        transition: transform .12s ease;
      }
      details[open] > summary > .c-name > .caret { transform: rotate(90deg); }
      .caret.spacer { border-left-color: transparent; }
      .tree-controls {
        display: flex;
        align-items: baseline;
        gap: .75rem;
        margin-top: 16px;
      }
      .segmented {
        display: inline-flex;
        border: 1px solid #cbd5e1;
        border-radius: 8px;
        overflow: hidden;
      }
      .segmented button {
        font: inherit;
        font-size: .85rem;
        color: #475569;
        background: #fff;
        border: 0;
        border-right: 1px solid #e2e8f0;
        padding: .35rem .85rem;
        cursor: pointer;
      }
      .segmented button:last-child { border-right: 0; }
      .segmented button:hover { background: oklch(99.087% 0.00442 33.09); }
      .segmented button.active { background: #2563eb; color: #fff; }
      .tree-controls .hint { font-size: .82rem; color: #94a3b8; }
      .empty {
        margin-top: 16px;
        padding: 1rem;
        border: 1px dashed #cbd5e1;
        border-radius: 12px;
        color: #64748b;
        background: oklch(99.087% 0.00442 33.09);
      }
      @media print {
        body { background: #fff; color: #000; font-size: 0.8em; }
        table, .tree { font-size: 0.8em; }
        .tree-controls { display: none; }
        details.node { break-inside: avoid; }
        h1 { font-size: 1.5em; }
        h2 { font-size: 1.2em; }
      }
    </style>
  </head>
  <body>
    <h1>Kraken2 Taxonomic Classification</h1>
    <p class="subtitle"><b>Sample:</b> {{ sample_id }}</p>

    <div class="cards">
      <div class="card">
        <div class="label">Total reads</div>
        <div class="value">{{ "{:,}".format(summary.total_reads) }}</div>
      </div>
      <div class="card">
        <div class="label">Classified</div>
        <div class="value">{{ "%.2f"|format(summary.classified_pct) }}%</div>
        <div class="sub">{{ "{:,}".format(summary.classified_reads) }} reads</div>
      </div>
      <div class="card">
        <div class="label">Unclassified</div>
        <div class="value">{{ "%.2f"|format(summary.unclassified_pct) }}%</div>
        <div class="sub">{{ "{:,}".format(summary.unclassified_reads) }} reads</div>
      </div>
      <div class="card">
        <div class="label">Distinct taxa</div>
        <div class="value">{{ "{:,}".format(summary.distinct_taxa) }}</div>
      </div>
      {% if summary.top_species %}
      <div class="card">
        <div class="label">Top species</div>
        <div class="value" style="font-size:1rem">{{ summary.top_species.name }}</div>
        <div class="sub">{{ "%.2f"|format(summary.top_species.pct) }}% of reads</div>
      </div>
      {% endif %}
    </div>

    {% if top_species %}
    <h2>Top species</h2>
    <table>
      <thead>
        <tr>
          <th>Species</th>
          <th class="num">Clade %</th>
          <th style="width:120px">Share</th>
          <th class="num">Reads</th>
          <th class="num">TaxID</th>
        </tr>
      </thead>
      <tbody>
        {% for row in top_species %}
        <tr>
          <td>{{ row.name }}</td>
          <td class="num">{{ "%.2f"|format(row.pct) }}</td>
          <td>
            <div class="bar"><span style="width: {{ row.pct }}%"></span>
              <em>{{ "%.2f"|format(row.pct) }}%</em></div>
          </td>
          <td class="num">{{ "{:,}".format(row.clade_reads) }}</td>
          <td class="num">{{ row.taxid }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% endif %}

    <h2>Full taxonomic breakdown</h2>
    {% if has_taxa %}
    <div class="tree-controls">
      <div class="segmented" role="group" aria-label="Tree detail level">
        <button type="button" data-mode="relevant" class="active"
                onclick="setMode('relevant')">Relevant</button>
        <button type="button" data-mode="expanded"
                onclick="setMode('expanded')">Expanded</button>
        <button type="button" data-mode="collapsed"
                onclick="setMode('collapsed')">Collapsed</button>
      </div>
      <span class="hint">Relevant shows the prevalent lineage; click any row
        to drill in.</span>
    </div>
    <div class="tree">
      <div class="tree-head">
        <span>Taxon</span>
        <span>Rank</span>
        <span class="num">Clade %</span>
        <span class="num">Clade reads</span>
        <span class="num">Direct reads</span>
        <span class="num">TaxID</span>
      </div>
      {{ tree_html | safe }}
    </div>
    <script>
      function setMode(mode) {
        document.querySelectorAll("details.node").forEach(function (node) {
          if (mode === "expanded") {
            node.open = true;
          } else if (mode === "collapsed") {
            node.open = false;
          } else {
            node.open = node.dataset.default === "open";
          }
        });
        document.querySelectorAll(".segmented button").forEach(function (btn) {
          btn.classList.toggle("active", btn.dataset.mode === mode);
        });
      }
    </script>
    {% else %}
    <div class="empty">No classifications were reported for this sample.</div>
    {% endif %}
  </body>
</html>
"""


def render_kraken2_report_html(report_text: str, sample_id: str) -> str:
    """Render the Kraken2 report text into a self-contained HTML document."""
    rows = parse_kraken2_report(report_text)
    summary = summarize(rows)
    return Template(KRAKEN2_REPORT_TEMPLATE, autoescape=True).render(
        sample_id=html.escape(sample_id),
        summary=summary,
        top_species=top_species(rows),
        tree_html=render_tree_html(rows),
        has_taxa=bool(rows),
    )


def _split_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Split an s3://bucket/prefix URI into (bucket, key_prefix)."""
    without_scheme = s3_uri.split("s3://", 1)[-1]
    bucket, _, key_prefix = without_scheme.partition("/")
    return bucket, key_prefix.strip("/")


def _read_s3_text_or_none(s3_client, bucket: str, key: str) -> str | None:
    """Read an S3 text object, returning None if it does not exist.

    A missing object or bucket (NoSuchKey / 404 / NoSuchBucket) yields None so
    the caller can treat an absent kraken2 report as "no classifications"
    rather than an error; other client errors propagate.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as err:
        code = err.response["Error"]["Code"]
        if code in ("NoSuchKey", "404", "NoSuchBucket"):
            return None
        raise
    return response["Body"].read().decode("utf-8")


@task
def generate_and_store_kraken2_report(**context) -> str:
    """
    Build the kraken2 taxonomic-classification HTML report and store it in S3.

    Reads the kraken2 tool report file for the sample from the bactopia
    --outdir (<outdir>/<sample>/tools/kraken2/<sample>.kraken2.report.txt),
    renders a self-contained HTML tree, and writes it to
    s3://{REPORT_OUTPUT_BUCKET}/{REPORT_OUTPUT_PREFIX}/<sample>/kraken2.html.

    The kraken2 profile runs on the bactopia results, so it reports on the same
    bactopia `--sample`.

    Fails silently: if the report file is missing or empty there are no
    classifications to report, so the task logs and returns "" without writing
    or raising, leaving the DAG green.

    Args:
        context: Airflow context (automatically passed by TaskFlow API). The
                 sample id and outdir are read from the validated configs in
                 XCom under configs['bactopia']['nextflowOptions'].

    Returns:
        The S3 URI the report HTML was written to, or "" when skipped.
    """
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    bactopia_options = configs["bactopia"]["nextflowOptions"]
    sample_id = bactopia_options["--sample"]
    outdir = bactopia_options["--outdir"]

    src_bucket, src_prefix = _split_s3_uri(outdir)
    relative_key = KRAKEN2_REPORT_SOURCE_TEMPLATE.format(sample=sample_id)
    src_key = f"{src_prefix}/{relative_key}" if src_prefix else relative_key

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    report_text = _read_s3_text_or_none(s3_client, src_bucket, src_key)
    if not report_text or not report_text.strip():
        log.info(
            "No kraken2 report at s3://%s/%s for sample '%s'; skipping "
            "(no classifications to report).",
            src_bucket,
            src_key,
            sample_id,
        )
        return ""

    report_html = render_kraken2_report_html(report_text, sample_id)
    out_key = (
        f"{REPORT_OUTPUT_PREFIX}/{sample_id}/{KRAKEN2_REPORT_OUTPUT_FILENAME}"
    )
    s3_client.put_object(
        Bucket=REPORT_OUTPUT_BUCKET,
        Key=out_key,
        Body=report_html.encode("utf-8"),
        ContentType="text/html",
    )
    out_uri = f"s3://{REPORT_OUTPUT_BUCKET}/{out_key}"
    log.info("Wrote kraken2 report for sample '%s' to %s", sample_id, out_uri)
    return out_uri


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

    # Submit bactopia without blocking so the kraken2 branch can start as soon
    # as the sample's QC output is available while bactopia keeps running. A
    # BatchSensor (below) waits for the job to actually finish before the
    # report step.
    submit_bactopia_job = BatchOperator(
        task_id="submit_bactopia_batch_job",
        job_name=("{{ dag_run.dag_id }}-bactopia-to-kraken2-wf-bactopia-job"),
        job_queue=WORKFLOW_QUEUE_NAME,
        job_definition=NEXTFLOW_JOB_DEFINITION,
        wait_for_completion=False,
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

    # Gate kraken2 on the bactopia QC output for this sample. bactopia's first
    # pipeline step writes <outdir>/<sample>/main/qc/<sample>.fastq.gz (the
    # --outdir already includes the pipeline-output segment), so kraken2 can
    # classify the QC'd reads while the rest of bactopia is still running.
    bactopia_opts = (
        "ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')"
        "['bactopia']['nextflowOptions']"
    )
    bactopia_outdir = f"{{{{ {bactopia_opts}['--outdir'] }}}}"
    bactopia_sample = f"{{{{ {bactopia_opts}['--sample'] }}}}"
    qc_output_key = (
        f"{bactopia_outdir}/{bactopia_sample}"
        f"/main/qc/{bactopia_sample}.fastq.gz"
    )

    wait_for_qc_output = S3KeySensor(
        task_id="wait_for_bactopia_qc_output",
        bucket_key=qc_output_key,
        poke_interval=30,
        mode="reschedule",
    )

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

    # Bactopia report generation. Once bactopia finishes, its results have been
    # written to the seqauto result-raw bucket and are being transformed into
    # result-clean by asynchronous Glue ETL jobs. This task runs a
    # crawl-then-probe loop against the input-clean and result-clean crawlers
    # and the canned-report lambda, then writes the rendered HTML to S3. It
    # depends only on the bactopia branch, so a kraken2 failure does not block
    # it. See generate_and_store_report and the report constants for details.
    #
    # NOTE: this stores the report at a fixed S3 location and does not carry any
    #       authn context; the download-time authz concerns of the existing API
    #       endpoint are intentionally out of scope for this workflow step.
    generate_report = generate_and_store_report()

    # bactopia was submitted non-blocking; wait for the job to finish before
    # the report, which depends on the full bactopia results.
    wait_for_bactopia_complete = BatchSensor(
        task_id="wait_for_bactopia_complete",
        job_id="{{ ti.xcom_pull(task_ids='submit_bactopia_batch_job') }}",
        poke_interval=30,
        mode="reschedule",
    )

    # Dependency graph. bactopia and kraken2 run in parallel: kraken2 starts as
    # soon as the QC output and include file are ready, while bactopia keeps
    # running. The two report branches are independent - the bactopia report
    # waits only for the full bactopia job, and the kraken2 report waits only
    # for the kraken2 job - so a failure in one branch does not block the
    # other.
    #
    #   configs -> submit_bactopia_job -> wait_for_bactopia_qc_output
    #                                  -> wait_for_bactopia_complete
    #   configs -> create_k2_include   -> wait_for_kraken_2_include_file
    #   [qc_output, k2_include]        -> submit_kraken2_job
    #   wait_for_bactopia_complete     -> generate_report
    #   submit_kraken2_job             -> generate_kraken2_report
    chain(
        configs,
        submit_bactopia_job,
        [wait_for_qc_output, wait_for_bactopia_complete],
    )
    chain(configs, create_k2_include, wait_for_k2_include)
    chain([wait_for_qc_output, wait_for_k2_include], submit_kraken2_job)
    chain(wait_for_bactopia_complete, generate_report)

    # After kraken2 finishes (submit_kraken2_job waits for completion), build
    # the taxonomic report directly from the kraken2 tool report file and push
    # it to S3 alongside the bactopia report. It depends only on
    # submit_kraken2_job, so it runs independently of the bactopia report and
    # fails silently if the report file is missing or empty.
    generate_kraken2_report = generate_and_store_kraken2_report()
    chain(submit_kraken2_job, generate_kraken2_report)


bactopia_and_kraken2_v3_2_0()
