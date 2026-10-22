# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "62442c7c-1761-4f15-9348-d08cfa834afd",
# META       "default_lakehouse_name": "lh_STDFB",
# META       "default_lakehouse_workspace_id": "89c61aec-8545-450e-8825-a38b1903e3bd",
# META       "known_lakehouses": [
# META         {
# META           "id": "62442c7c-1761-4f15-9348-d08cfa834afd"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "a70c974d-cb0f-883a-4c39-cfb082c59e32",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(227, 227, 227, 1); padding: 16px; border-radius: 6px;">
#   <h1>Hi</h1>
# 
#   <p style="margin-bottom: 16px;">
#     This notebook contains the Alkali data pipelines for project nusync_events_rss.
#   </p>
#   <p style="margin-bottom: 16px;">
#     Alkali Engine: alkali1
#   </p>
#   <p style="margin-bottom: 8px;">
#   AI Information:
#     <ol>
#       <li>Generate Date: 2026-05-06 12:00:00 </li>
#       <li>Agent Name:    Alkali Data Platform, unversioned </li>
#       <li>Skill1 Name:   alkali-dc-dqc-pc, unversioned </li>      <li>Skill2 Name:   alkali-activity-creator, unversioned </li>    </ol>
#   </p>
# </div>

# MARKDOWN ********************

# <div style="background-color: rgba(230, 230, 230, 1); padding: 16px; border-radius: 6px;">
#   <h3>Parameters</h3>
#   <p>
#     From calling parent pipeline
#   </p>
# </div>

# PARAMETERS CELL ********************

# Parameters from the parent pipeline
BATCH_WORKSPACE_ID = "My parent workspace ID"
BATCH_PIPELINE_NAME = "My parent pipeline name"
BATCH_PIPELINE_RUN_ID = "My parent pipeline ID"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(230, 230, 230, 1); padding: 16px; border-radius: 6px;">
#   <h3>Local Configuration</h3>(Don't modify normally)
# </div>

# CELL ********************

#
EXEC_ENV = "DEV"
#EXEC_WS_ID = "89c61aec-8545-450e-8825-a38b1903e3bd"



# -----------------------
# log level (log)
# -----------------------
LOG_LEVEL_DEBUG="DEBUG"
LOG_LEVEL_INFO="INFO"
LOG_LEVEL_WARN="WARN"
LOG_LEVEL_Error="ERROR"

# Data store (DS)
DS_TYPE_LAKEHOUSE = "LAKEHOUSE"
DS_TYPE_WAREHOUSE = "WAREHOUSE"

NOTEBOOK_LOGGER_NAME="notebook_logger",# any logical logger name
NOTEBOOK_LOGGER_LEVEL="DEBUG",                 # or "INFO", "ERROR", etc.
NOTEBOOK_LOG_TABLE_PATH = "etl_log/log_notebook"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get default data stores (default ds)

DEBUG_MODE_F = True

DS_CFG_ENV = "DEV"
DS_CFG_VER = '1.0.0'

DS_CFG_WS_NAME = "MSF-POC-DE"
DS_CFG_WS_ID = "aed8d644-b1a8-43d8-97fe-dd551056ae5c"
DS_CFG_DS_TYPE = "LAKEHOUSE"
DS_CFG_DS_NAME = "lh_etl"
DS_CFG_DS_ID = "4030e7ed-d335-4929-a9e8-beedda9855d1"
DS_CFG_SCHEMA = "etl_config"
DS_CFG_TABLE = "ds"




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(230, 230, 230, 1); padding: 16px; border-radius: 6px;">
#   <h3>Packages</h3>
# </div>

# CELL ********************

from alkali1.cfg.ds_cfg import get_default_ds_cfg
from alkali1.cfg.ds_cfg import update_ds_cfg
from alkali1.cfg.ds_cfg import get_ds_cfg

from alkali1.common.logger import get_logger, RunStatus
import alkali1.pc.engine as engine_mod
import alkali1.pc.engine.activities as activities_mod
from alkali1.pc.runtime import run_pc
from alkali1.pc.pc import build_param_pc, ParamPCBuildError

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(225, 224, 255, 1); padding: 16px; border-radius: 6px;">
#   <h3>Local activities (act)</h3>
# </div>

# CELL ********************

# Add your local activities here

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(224, 224, 224, 1); padding: 16px; border-radius: 6px;">
#    <h3>Local debug helpers</h3>(Don't modify)
#  </div>

# CELL ********************

#
# Local debug helpers (Please don't modify this cell)
#

from __future__ import annotations

# ---------------------------------------------
# Alkali Debug Display Helper (Notebook)
# - Per activity/output: show result_df then df
# - Header format (3 lines):
#     Pipeline Name: "..."
#     Activity Name: "..."
#     Output Name:   "..."  Total: ... Displaying: ...
# - Sort order: pipeline_seq -> activity_seq -> stable insertion order
# - Blank lines between debug outputs for readability
# ---------------------------------------------

# ============================================================================
# AI CONTRACT - ALKALI DEBUG DISPLAY HELPER (AUTHORITATIVE)
# ============================================================================
# Version: v2.6.0
#
# PURPOSE
# -------
# Render debug outputs registered by alkali1.pc.engine.debug in notebooks.
#
# INPUT SOURCES
# -------------
# - DF_REGISTRY, DF_DIMENSIONS, DF_DISPLAY_ROWS
# - RESULT_REGISTRY
# - ACTIVITY_NAME_BY_OUTPUT
# - PIPELINE_NAME_BY_OUTPUT
# - PIPELINE_SEQ_BY_OUTPUT
# - ACTIVITY_SEQ_BY_OUTPUT
#
# NAMING RULES
# ------------
# - Supports scoped output bases from parallel PC:
#     <pipeline_name>::<output_base>
# - Internal lookup always uses scoped base when present.
# - Header output name displays logical base only (without pipeline prefix).
#
# DISPLAY RULES
# -------------
# - Per base display order:
#     1) <base>.result_df.df
#     2) <base>.df
# - Overall ordering when name is not provided:
#     pipeline_seq -> activity_seq -> stable insertion order
#
# FILTER RULES
# ------------
# - name="<scoped>" matches exact scoped base.
# - name="<unscoped>" matches all bases whose logical base equals the name.
#
# GUARANTEES
# ----------
# - No key collision in display iteration for parallel pipelines with same output names.
# - Backward compatible with non-scoped historical keys.
# ============================================================================

# Primary source: centralized debug module
from alkali1.pc.engine import debug as dbg

DF_REGISTRY = dbg.DF_REGISTRY
DF_DIMENSIONS = dbg.DF_DIMENSIONS
DF_DISPLAY_ROWS = dbg.DF_DISPLAY_ROWS

# Optional: result registry (for best-effort inference)
RESULT_REGISTRY = getattr(dbg, "RESULT_REGISTRY", {})

# Activity name mapping
ACTIVITY_NAME_BY_OUTPUT = getattr(dbg, "ACTIVITY_NAME_BY_OUTPUT", {})

# Pipeline + ordering registries (best-effort, backward compatible)
PIPELINE_NAME_BY_OUTPUT = getattr(dbg, "PIPELINE_NAME_BY_OUTPUT", {})
PIPELINE_SEQ_BY_OUTPUT = getattr(dbg, "PIPELINE_SEQ_BY_OUTPUT", {})
ACTIVITY_SEQ_BY_OUTPUT = getattr(dbg, "ACTIVITY_SEQ_BY_OUTPUT", {})

VERSION = "v2.6.0"

_RESULT_DF_SUFFIX = ".result_df.df"   # per debug.py canonical naming
_DF_SUFFIX = ".df"
_RESULT_SUFFIX = ".result"
_SCOPE_SEP = "::"


def _spacer(n: int = 1) -> None:
    for _ in range(max(1, int(n))):
        print("")


def _split_scoped_base(base: str) -> tuple[str | None, str]:
    b = (base or "").strip()
    if not b:
        return None, b
    if _SCOPE_SEP in b:
        pipe, logical = b.split(_SCOPE_SEP, 1)
        pipe = pipe.strip() or None
        logical = logical.strip()
        return pipe, logical
    return None, b


def _logical_base(base: str) -> str:
    _, logical = _split_scoped_base(base)
    return logical


def _to_base(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return n
    if n.endswith(_RESULT_DF_SUFFIX):
        return n[: -len(_RESULT_DF_SUFFIX)]
    if n.endswith(_RESULT_SUFFIX):
        return n[: -len(_RESULT_SUFFIX)]
    if n.endswith(_DF_SUFFIX):
        return n[: -len(_DF_SUFFIX)]
    return n


def _keys_for_base(base: str) -> tuple[str, str, str]:
    base = base.strip()
    result_df_key = f"{base}{_RESULT_DF_SUFFIX}"
    df_key = f"{base}{_DF_SUFFIX}"
    result_name_key = f"{base}{_RESULT_SUFFIX}"
    return result_df_key, df_key, result_name_key


def _dims_str(df_key: str) -> tuple[str, str]:
    dims = DF_DIMENSIONS.get(df_key)
    total_rows, total_cols = dims if isinstance(dims, tuple) and len(dims) == 2 else (None, None)
    display_rows = DF_DISPLAY_ROWS.get(df_key)

    total_rows_str = str(total_rows) if total_rows is not None else "?"
    total_cols_str = str(total_cols) if total_cols is not None else "?"
    display_rows_str = str(display_rows) if display_rows is not None else "?"

    total_str = f"Total: {total_rows_str} rows x {total_cols_str} columns"
    display_str = f"Displaying: top {display_rows_str} rows"
    return total_str, display_str


def _infer_activity_name(base_output: str) -> str:
    mapping = ACTIVITY_NAME_BY_OUTPUT
    if isinstance(mapping, dict) and base_output in mapping:
        v = mapping.get(base_output)
        if isinstance(v, str) and v.strip():
            return v.strip()

    res_key = f"{base_output}{_RESULT_SUFFIX}"
    payload = RESULT_REGISTRY.get(res_key)
    if isinstance(payload, dict):
        v = payload.get("activity_name")
        if isinstance(v, str) and v.strip():
            return v.strip()

    return _logical_base(base_output) or base_output


def _infer_pipeline_name(base_output: str) -> str:
    mapping = PIPELINE_NAME_BY_OUTPUT
    if isinstance(mapping, dict) and base_output in mapping:
        v = mapping.get(base_output)
        if isinstance(v, str) and v.strip():
            return v.strip()

    res_key = f"{base_output}{_RESULT_SUFFIX}"
    payload = RESULT_REGISTRY.get(res_key)
    if isinstance(payload, dict):
        v = payload.get("pipeline_name")
        if isinstance(v, str) and v.strip():
            return v.strip()

    scoped_pipe, _ = _split_scoped_base(base_output)
    if scoped_pipe:
        return scoped_pipe

    return "?"


def _infer_pipeline_seq(base_output: str) -> int | None:
    mapping = PIPELINE_SEQ_BY_OUTPUT
    if isinstance(mapping, dict) and base_output in mapping:
        try:
            return int(mapping.get(base_output))
        except Exception:
            return None

    res_key = f"{base_output}{_RESULT_SUFFIX}"
    payload = RESULT_REGISTRY.get(res_key)
    if isinstance(payload, dict) and "pipeline_seq" in payload:
        try:
            return int(payload.get("pipeline_seq"))
        except Exception:
            return None

    return None


def _infer_activity_seq(base_output: str) -> int | None:
    mapping = ACTIVITY_SEQ_BY_OUTPUT
    if isinstance(mapping, dict) and base_output in mapping:
        try:
            return int(mapping.get(base_output))
        except Exception:
            return None

    res_key = f"{base_output}{_RESULT_SUFFIX}"
    payload = RESULT_REGISTRY.get(res_key)
    if isinstance(payload, dict) and "activity_seq" in payload:
        try:
            return int(payload.get("activity_seq"))
        except Exception:
            return None

    return None


def _print_custom_header(
    pipeline_name: str,
    activity_name: str,
    output_name: str,
    df_key_for_dims: str,
):
    total_str, display_str = _dims_str(df_key_for_dims)

    pipeline_name = (pipeline_name or "?").strip() or "?"
    activity_name = (activity_name or "?").strip() or "?"
    output_name = (output_name or "?").strip() or "?"

    print(f'Pipeline Name: "{pipeline_name}"')
    print(f'Activity Name: "{activity_name}"')
    print(f'Output Name:   "{output_name}"  {total_str}   {display_str}')


def _collect_bases_in_order() -> list[str]:
    bases = []
    seen = set()

    for key in DF_REGISTRY.keys():
        if key.endswith(_RESULT_DF_SUFFIX):
            base = key[: -len(_RESULT_DF_SUFFIX)]
        elif key.endswith(_DF_SUFFIX):
            base = key[: -len(_DF_SUFFIX)]
        else:
            base = key

        if base and base not in seen:
            bases.append(base)
            seen.add(base)

    return bases


def _select_bases_by_name(input_name: str) -> list[str]:
    requested = _to_base(input_name)
    if not requested:
        return []

    all_bases = _collect_bases_in_order()

    if _SCOPE_SEP in requested:
        return [b for b in all_bases if b == requested]

    selected = [b for b in all_bases if _logical_base(b) == requested or b == requested]
    return selected if selected else [requested]


def display_debug(name: str | None = None, *, show_missing: bool = False):
    print(f"[display_debug] helper version={VERSION}")

    if not DF_REGISTRY:
        print("No DataFrames registered. Did you run run_pc() with debug_mode=True?")
        return

    if name:
        bases = _select_bases_by_name(name)
    else:
        bases = _collect_bases_in_order()

        indexed = list(enumerate(bases))
        big = 10**12

        def sort_key(t):
            i, b = t
            pseq = _infer_pipeline_seq(b)
            aseq = _infer_activity_seq(b)
            return (
                pseq if pseq is not None else big,
                aseq if aseq is not None else big,
                i,
            )

        indexed.sort(key=sort_key)
        bases = [b for _, b in indexed]

    first_group = True
    for base in bases:
        if not base:
            continue

        result_df_key, df_key, _ = _keys_for_base(base)
        logical = _logical_base(base)

        pipe_name = _infer_pipeline_name(base)
        act_name = _infer_activity_name(base)

        has_result_df = result_df_key in DF_REGISTRY
        has_df = df_key in DF_REGISTRY

        if not (has_result_df or has_df):
            if show_missing:
                if not first_group:
                    _spacer(1)
                _print_custom_header(pipe_name, act_name, logical, base)
                print("(no registered debug DFs)")
            continue

        if not first_group:
            _spacer(2)
        first_group = False

        if has_result_df:
            _spacer(1)
            _print_custom_header(pipe_name, act_name, f"{logical}.result", result_df_key)
            display(DF_REGISTRY[result_df_key])
        elif show_missing:
            _spacer(1)
            print(f'Pipeline Name: "{pipe_name}"')
            print(f'Activity Name: "{act_name}"')
            print(f'Output Name:   "{logical}.result"  (missing result_df)')

        if has_result_df and has_df:
            _spacer(1)

        if has_df:
            _print_custom_header(pipe_name, act_name, f"{logical}.df", df_key)
            display(DF_REGISTRY[df_key])
        elif show_missing:
            _spacer(1)
            print(f'Pipeline Name: "{pipe_name}"')
            print(f'Activity Name: "{act_name}"')
            print(f'Output Name:   "{logical}.df"  (missing df)')

        _spacer(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(225, 224, 255, 1); padding: 16px; border-radius: 6px;">
#    <h3>Pipeline Contract (PC)</h3>
#  </div>

# CELL ********************



pc_string = """
{
  "pc_kind": "main",
  "pc_version": "1.0.0",
  "parallel_pipelines_limit": 1,
  "pipelines": [
    {
      "pipeline_name": "nusync_events_rss_http_to_bronze",
      "activities": [
        
        {
          "activity_type": "io.append_file_timestamp",
          "activity_name": "10.append_timestamp_to_landing_files",
          "params": {
            "ds_alias": "landing_ds",
            "folder_path": "Files/test/csv",
            "overwrite": false,
            "dry_run": false
          },
          "output": "landing_append_timestamp_result"
        },
        {
          "activity_type": "io.load_file_folder",
          "activity_name": "20.load_landing_csv_to_bronze_stg_overwrite",
          "params": {
            "src_ds": "landing_ds",
            "ctrl_ds": "etl_ctrl",
            "src_folder_path": "Files/test/csv",
            "src_file_format": "csv",
            "tgt_ds": "bronze_stg_ds",
            "tgt_schema": "test_csv",
            "tgt_table": "customer_addr_raw",
            "tgt_mode": "overwrite"
          },
          "output": "bronze_stg_overwrite_result"
        },
        {
          "activity_type": "dq.check_table_rules",
          "activity_name": "30.dq_bronze_stg_contactemail_not_null",
          "params": {
            "src_ds": "bronze_stg_ds",
            "src_schema": "test_csv",
            "src_table": "customer_addr_raw",
            "fail_on_blocking": true,
            "rules": [
              {
                "rule_id": "DQ001",
                "target_type": "lakehouse_table",
                "rule_type": "completeness",
                "severity": "blocking",
                "column": "CUSTOMER_ID",
                "expectation": "not_null"
              }
            ]
          },
          "output": "dq_bronze_stg_result"
        },
        {
          "activity_type": "io.read_table",
          "activity_name": "40.read_bronze_stg_table",
          "params": {
            "src_ds": "bronze_stg_ds",
            "src_schema": "test_csv",
            "src_table": "customer_addr_raw"
          },
          "output": "bronze_stg_read_result"
        },
        {
          "activity_type": "io.write_df",
          "activity_name": "50.publish_bronze_stg_to_bronze_append",
          "params": {
            "src_df": "bronze_stg_read_result",
            "tgt_ds": "bronze_ds",
            "tgt_schema": "s_test_csv",
            "tgt_table": "customer_addr_2",
            "mode": "append",
            "auto_create_table_f": true
          },
          "output": "bronze_publish_append_result"
        },
        {
          "activity_type": "debug.stop",
          "activity_name": "60.debug_stop"
        }
      ]
    }
  ]
}
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(221, 249, 205, 1); padding: 16px; border-radius: 6px;">
#   <h3>Orchestration</h3>
# </div>

# CELL ********************


# Get default ds configuration

default_ds_dict  = get_default_ds_cfg(
    spark=spark,
    env=EXEC_ENV,
    ver=DS_CFG_VER,
    cfg_ws_id=DS_CFG_WS_ID,
    cfg_ws_name=DS_CFG_WS_NAME,
    cfg_ds_type=DS_CFG_DS_TYPE,
    cfg_ds_id=DS_CFG_DS_ID,
    cfg_ds_name=DS_CFG_DS_NAME,
    cfg_ds_schema=DS_CFG_SCHEMA,
    cfg_ds_table=DS_CFG_TABLE,
    retry_number=0,
    logger=None,
    fail_on_missing_alias=False
)

print(default_ds_dict)


# Now you may use update_ds_cfg() to update a ds if needed
"""
update_ds_cfg(
    spark,
    ds_stores=default_ds_dict,
    ds_alias=?,
    ds_type=?,
    ws_name=?,
    ws_id=?,
    ds_name=?,
    ds_id=?,
    logger=?,
)
"""

logger_ds_cfg = get_ds_cfg(spark=spark,
                           ds_stores=default_ds_dict,
                           ds_alias="log_ds")
#
# Create a notebook logger
#

logger = get_logger(
    spark=spark,
    name="notebook_logger",            # any logical logger name
    log_level="DEBUG",                 # or "INFO", "ERROR", etc.

    # Lakehouse Delta log sink (from your ABFSS path)
    lh_workspace_id=logger_ds_cfg["ws_id"],
    lh_id=logger_ds_cfg["ds_id"],
    lh_table_path=NOTEBOOK_LOG_TABLE_PATH,              # path under /Tables

    # Optional pipeline context (helps later in analysis)
    batch_workspace_id=BATCH_WORKSPACE_ID,
    batch_pipeline_name=BATCH_PIPELINE_NAME,
    batch_pipeline_id=BATCH_PIPELINE_RUN_ID,        # e.g. your pipeline run_id
)

logger.info("Notebook started")

logger.info(default_ds_dict)


# runtime parameters
#

runtime_param_dict = {
    "execution": {
        "workspace_id": logger_ds_cfg["ws_id"],
        "environment": EXEC_ENV,
        "pipeline_run_id": BATCH_PIPELINE_RUN_ID,
        "debug_mode": DEBUG_MODE_F
    },

    "datastores": default_ds_dict
}

# print(f"runtime_param_dict: {runtime_param_dict}")


# Execute the PC
#
summary = run_pc(
    spark=spark,
    pc_str=pc_string,
    env_param=runtime_param_dict,
    logger=logger
)

# PC completed
logger.info(f"PC completed: {summary}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# <div style="background-color: rgba(228, 228, 228, 1); padding: 16px; border-radius: 6px;">
#   <h3>Display Debug Info</h3>
# </div>

# CELL ********************


# Diplay debug info (Only print the output when DEBUG_MODE_F == True)
#
display_debug()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

