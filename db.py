"""SQLite access for JobHunter AI — public API (implementation split across db_*.py)."""

from db_config import get_config, save_config
from db_init import check_db_connection, init_db
from db_jobs import (
    add_job,
    add_jobs_bulk,
    cleanup_old_jobs,
    delete_all_jobs,
    count_by_role,
    count_by_source,
    get_all_jobs,
    get_job_count,
    get_jobs_today,
    get_today_count,
    url_exists,
)
from db_paths import DATA_DIR, DB_PATH, DEFAULT_CONFIG, DEFAULT_SOURCES, ensure_data_dir
from db_runs import create_run, get_runs, update_run

__all__ = [
    "DATA_DIR",
    "DB_PATH",
    "DEFAULT_CONFIG",
    "DEFAULT_SOURCES",
    "add_job",
    "add_jobs_bulk",
    "check_db_connection",
    "cleanup_old_jobs",
    "delete_all_jobs",
    "count_by_role",
    "count_by_source",
    "create_run",
    "ensure_data_dir",
    "get_all_jobs",
    "get_config",
    "get_job_count",
    "get_jobs_today",
    "get_runs",
    "get_today_count",
    "init_db",
    "save_config",
    "update_run",
    "url_exists",
]
