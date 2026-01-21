import sqlite3
import json
import os
import sys
import argparse
import pickle
import datetime
import datetime as _dt
import shutil
import tempfile
import logging
import re
import glob
from apscheduler.triggers.combining import OrTrigger, AndTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.job import Job
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil import parser
from pathlib import Path
from scheduler.usecases import job
from scheduler.usecases import report


DATE_TYPE = 'date'
WEEKLY_TYPE = 'weekly'
DAILY_TYPE = 'daily'
HOURLY_TYPE = 'hourly'
MONTHLY_TYPE = 'monthly'
HOURLY_INTERVAL = 'hours'
DAILY_INTERVAL = 'days'
WEEKLY_INTERVAL = 'weeks'
MONTHLY_INTERVAL = 'months'
INTERVAL_MAP = {'HOURLY': HOURLY_INTERVAL, 'DAILY': DAILY_INTERVAL}
DAY_NAME_MAP = {0: 'sun', 1: 'mon', 2: 'tue', 3: 'wed', 4: 'thu', 5: 'fri',6: 'sat'}
DAY_NUMBER_MAP = {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6}
WEEK_TO_MONTH_DAYS = {0: '1-7', 1: '8-14', 2: '15-21', 3: '22-28'}
MONTH_DAYS_TO_WEEK = {'1-7': 0, '8-14': 1, '15-21': 2, '22-28': 3}
WEEKS_OF_MONTH_VALUES = [0, 1, 2, 3, 'last']
SCHEDULER_MISFIRE_GRACE_TIME = 600
SCHEDULER_DB_FILE = '/app/db_files/jobs.sqlite'

def isodate_to_datetime(date):
    if date:
        return parser.parse(date, ignoretz=True)

def datetime_to_isodate(date):
    if date:
        return date.isoformat().split('+')[0] + 'Z'

def _get_schedule_params(schedule):
    if isinstance(schedule.trigger, IntervalTrigger):
        params = _get_interval_schedule_params(schedule)
    elif isinstance(schedule.trigger, CronTrigger):
        month_field = str(schedule.trigger.fields[1])
        day_of_week_field = str(schedule.trigger.fields[4])
        if day_of_week_field != '*' and month_field == '*':
            params = _get_weekly_cron_trigger_params(schedule)
        else:
            params = _get_monthly_schedule_params(schedule)
    else:
        if isinstance(schedule.trigger, OrTrigger):
            params = _get_every_nth_week_trigger_params(schedule)
        else:
            params = _get_weeks_of_month_trigger_params(schedule)
        params['interval'] = WEEKLY_INTERVAL
    return params

def _get_monthly_schedule_params(schedule):
    params = {}
    params['interval'] = MONTHLY_INTERVAL
    months_cron = str(schedule.trigger.fields[1])
    day_cron = str(schedule.trigger.fields[2])
    if months_cron == '*':
        params['frequency'] = 1
    elif '/' in months_cron:
        params['frequency'] = int(months_cron.split('/')[1])
    else:
        params['frequency'] = 12
    if day_cron == 'last':
        params['last_day_of_month'] = True
    else:
        params['day_of_month'] = int(day_cron)
    return params

def _get_interval_schedule_params(schedule):
    params = {}
    if schedule.trigger.interval.days < 1:
        params['interval'] = HOURLY_INTERVAL
        params['frequency'] = schedule.trigger.interval.seconds / 3600
    else:
        params['interval'] = DAILY_INTERVAL
        params['frequency'] = schedule.trigger.interval.days
    return params

def _get_every_nth_week_trigger_params(schedule):
    params = {}

    first_trigger = schedule.trigger.triggers[0]
    if isinstance(first_trigger, IntervalTrigger):
        params['frequency'] = schedule.trigger.triggers[0].interval.days / 7
        params['days_of_week'] = []
        for trigger in schedule.trigger.triggers:
            weekday = trigger.start_date.isoweekday()
            if weekday == 7:
                weekday = 0
            params['days_of_week'].append(weekday)
    else:
        weekdays = set()
        run_dates = []
        for trigger in schedule.trigger.triggers:
            if hasattr(trigger, 'run_date'):
                run_dates.append(trigger.run_date)
                weekday = trigger.run_date.isoweekday()
                if weekday == 7:
                    weekday = 0
                weekdays.add(weekday)

        params['days_of_week'] = sorted(list(weekdays))
        if len(run_dates) >= 2:
            run_dates.sort()
            weekday_dates = {}
            for dt in run_dates:
                wd = dt.isoweekday()
                if wd == 7:
                    wd = 0
                if wd not in weekday_dates:
                    weekday_dates[wd] = []
                weekday_dates[wd].append(dt)
            frequency = None
            for wd, dates in weekday_dates.items():
                if len(dates) >= 2:
                    interval_days = (dates[1] - dates[0]).days
                    frequency = interval_days / 7
                    break
            if frequency is None:
                frequency = 2  # Default to bi-weekly for short ranges
            params['frequency'] = frequency
        else:
            params['frequency'] = 2

    return params

def _get_weekly_cron_trigger_params(schedule):
    params = {}
    params['interval'] = WEEKLY_INTERVAL
    params['frequency'] = 1
    day_of_week_field = str(schedule.trigger.fields[4])
    day_names = day_of_week_field.split(',')
    params['days_of_week'] = [DAY_NUMBER_MAP[day] for day in day_names]
    return params

def _get_weeks_of_month_trigger_params(schedule):
    params = {}
    params['weeks_of_month'] = []
    for trigger in schedule.trigger.triggers:
        month_days = str(trigger.fields[2]).split(',')
        for week_range in month_days:
            if 'last' in week_range and 'last' not in params['weeks_of_month']:
                params['weeks_of_month'].append('last')
            elif 'last' not in week_range:
                params['weeks_of_month'].append(MONTH_DAYS_TO_WEEK[week_range])
        params['days_of_week'] = [DAY_NUMBER_MAP[day] for day in str(trigger.fields[4]).split(',')]
    return params

def _get_combo_trigger_start_date(trigger):
    if isinstance(trigger, AndTrigger):
        first_trigger = trigger.triggers[0]
        if hasattr(first_trigger, 'start_date'):
            return datetime_to_isodate(first_trigger.start_date)
        elif hasattr(first_trigger, 'run_date'):
            return datetime_to_isodate(first_trigger.run_date)
    else:
        dates = []
        for t in trigger.triggers:
            if hasattr(t, 'start_date'):
                dates.append(t.start_date)
            elif hasattr(t, 'run_date'):
                dates.append(t.run_date)
        if dates:
            earliest = min(dates)
            return datetime_to_isodate(earliest)
    return None

def _get_combo_trigger_end_date(trigger):
    first_trigger = trigger.triggers[0]
    if hasattr(first_trigger, 'end_date'):
        return datetime_to_isodate(first_trigger.end_date)
    dates = []
    for t in trigger.triggers:
        if hasattr(t, 'run_date'):
            dates.append(t.run_date)
    if dates:
        latest = max(dates)
        return datetime_to_isodate(latest)
    return None

def _schedule_to_dict(schedule):
    schedule_type = _get_schedule_type(schedule)
    params = None
    if schedule_type != DATE_TYPE:
        params = _get_schedule_params(schedule)
    if schedule_type == DATE_TYPE:
        start_time = datetime_to_isodate(schedule.trigger.run_date)
        end_time = None
    elif schedule_type in [HOURLY_TYPE, DAILY_TYPE, MONTHLY_TYPE]:
        start_time = datetime_to_isodate(schedule.trigger.start_date)
        end_time = datetime_to_isodate(schedule.trigger.end_date)
    elif schedule_type == WEEKLY_TYPE:
        if isinstance(schedule.trigger, CronTrigger):
            start_time = datetime_to_isodate(schedule.trigger.start_date)
            end_time = datetime_to_isodate(schedule.trigger.end_date)
        elif isinstance(schedule.trigger, (OrTrigger, AndTrigger)):
            start_time = _get_combo_trigger_start_date(schedule.trigger)
            end_time = _get_combo_trigger_end_date(schedule.trigger)
    else:
        start_time = _get_combo_trigger_start_date(schedule.trigger)
        end_time = _get_combo_trigger_end_date(schedule.trigger)
    next_run = datetime_to_isodate(schedule.next_run_time)
    d = dict(
        name=schedule.name,
        action=schedule.func.__name__,
        action_kwargs=schedule.kwargs.copy(),
        schedule_params=params,
        schedule_type=schedule_type,
        start_time=start_time,
        end_time=end_time,
        next_run=next_run)
    if 'display_name' in schedule.kwargs:
        d['display_name'] = schedule.kwargs['display_name']
        d['action_kwargs'].pop('display_name', None)
    if 'range' in schedule.kwargs:
        d['range'] = schedule.kwargs['range']
        d['action_kwargs'].pop('range', None)
    if 'individual_recipients' in schedule.kwargs:
        d['individual_recipients'] = schedule.kwargs['individual_recipients']
        d['action_kwargs'].pop('individual_recipients', None)
    return d

def _get_schedule_type(schedule):
    schedule_type = None
    if isinstance(schedule.trigger, DateTrigger):
        schedule_type = DATE_TYPE
    elif isinstance(schedule.trigger, (AndTrigger, OrTrigger)):
        schedule_type = WEEKLY_TYPE
    elif isinstance(schedule.trigger, IntervalTrigger):
        if schedule.trigger.interval_length % (24 * 60 * 60) == 0:
            schedule_type = DAILY_TYPE
        else:
            schedule_type = HOURLY_TYPE
    elif isinstance(schedule.trigger, CronTrigger):
        month_field = str(schedule.trigger.fields[1])
        day_of_week_field = str(schedule.trigger.fields[4])
        if day_of_week_field != '*' and month_field == '*':
            schedule_type = WEEKLY_TYPE
        else:
            schedule_type = MONTHLY_TYPE
    return schedule_type

def _get_schedule_func(action):
    if action == "run_schedule":
        func = run_schedule
    elif action == "report_schedule":
        func = report_schedule
    return func

def looks_like_apscheduler_job(obj):
    return (hasattr(obj, "id") and hasattr(obj, "trigger") and hasattr(obj, "next_run_time"))

def looks_like_apscheduler_trigger(obj):
    return (hasattr(obj, "__getstate__") or hasattr(obj, "fields"))

def serialize_apscheduler_job(job):
    return {
        "type": "APSchedulerJob",
        "id": getattr(job, "id", None),
        "name": getattr(job, "name", None),
        "func_ref": getattr(job, "func_ref", None),
        "trigger": make_value_serializable(getattr(job, "trigger", None)),
        "next_run_time": make_value_serializable(getattr(job, "next_run_time", None)),
        "kwargs": make_value_serializable(getattr(job, "kwargs", None)),
    }

def serialize_apscheduler_trigger(trigger):
    data = {
        "type": f"APSchedulerTrigger:{trigger.__class__.__name__}"
    }
    if hasattr(trigger, "__getstate__"):
        try:
            state = trigger.__getstate__()
            data["state"] = make_value_serializable(state)
        except Exception:
            pass
    if hasattr(trigger, "fields"):
        try:
            data["fields"] = {
                f.name: make_value_serializable(getattr(trigger, f.name))
                for f in trigger.fields
            }
        except Exception:
            pass
    return data


def make_value_serializable(value):
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    if looks_like_apscheduler_job(value):
        return serialize_apscheduler_job(value)
    if looks_like_apscheduler_trigger(value):
        return serialize_apscheduler_trigger(value)
    if isinstance(value, dict):
        return {k: make_value_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_value_serializable(v) for v in value]
    if isinstance(value, bytes):
        return value.hex()
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        return str(value)
    return value


def dump_table_to_json(db_path, table_name, output_dir):
    """
    Dump a table to JSON file.
    Returns: (success_count, error_count) tuple
    """
    con = None
    try:
        con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()

        processed_data = []
        for row in rows:
            row_dict = dict(row)
            if table_name == 'apscheduler_jobs' and 'job_state' in row_dict:
                job_state_bytes = row_dict.get('job_state')
                if isinstance(job_state_bytes, bytes):
                    try:
                        unpickled_state = pickle.loads(job_state_bytes)
                        row_dict['job_state'] = unpickled_state
                    except Exception:
                        logging.warning(
                            f"Could not unpickle job_state for a row in '{table_name}', falling back to hex.",
                            exc_info=True
                        )
                        row_dict['job_state'] = job_state_bytes.hex()
            serializable_row = {key: make_value_serializable(value) for key, value in row_dict.items()}
            processed_data.append(serializable_row)
        
        success_count = len(processed_data)
        error_count = 0
        db_name = Path(db_path).stem
        output_filename = os.path.join(output_dir, f"{db_name}_{table_name}.json")
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=4, ensure_ascii=False)
        logging.info(
            f"Successfully dumped table '{table_name}' to '{output_filename}': "
            f"{success_count} rows processed"
        )
    except Exception:
        logging.exception(f"Error processing table '{table_name}' from '{db_path}'")
        return (0, 1)
    finally:
        if con:
            con.close()
    
    return (success_count, error_count)


def load_apscheduler_job_state_with_sqlalchemy(db_path):
    """
    Load APScheduler jobs from database.
    Jobs that cannot be restored (e.g., missing modules) will be logged
    as errors by APScheduler but the function will continue and return
    only the successfully restored jobs.
    """
    jobstore = SQLAlchemyJobStore(url=f'sqlite:///{db_path}')
    jobstores = {'default': jobstore}
    scheduler = BackgroundScheduler(jobstores=jobstores)
    try:
        scheduler.start()
        jobs = scheduler.get_jobs()
        return jobs
    finally:
        # Ensure scheduler is properly shut down
        if scheduler.running:
            scheduler.shutdown(wait=False)

def dump_apscheduler_jobs_to_dict(db_path, output_dir):
    """
    Dump APScheduler jobs to JSON file.
    Returns: (success_count, error_count) tuple
    """
    try:
        jobs = load_apscheduler_job_state_with_sqlalchemy(db_path)
        jobs_data = []
        
        for job in jobs:
            schedule_dict = _schedule_to_dict(job)
            jobs_data.append(schedule_dict)
        
        success_count = len(jobs_data)
        error_count = 0
        
        # Use database name in filename to avoid overwriting when processing multiple databases
        db_name = Path(db_path).stem
        schedule_filename = os.path.join(output_dir, f"{db_name}_as_api_resource.json")
        with open(schedule_filename, "w", encoding="utf-8") as f:
            json.dump(jobs_data, f, indent=4, ensure_ascii=False)
        logging.info(
            f"APScheduler jobs dumped to {schedule_filename}: "
            f"{success_count} jobs processed, {error_count} errors"
        )
        return (success_count, error_count)
    except Exception as e:
        logging.exception(f"Error dumping APScheduler jobs from '{db_path}': {e}")
        return (0, 1)

def get_tables(db_path):
    con = None
    try:
        con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cur.fetchall()]
        return tables
    except sqlite3.Error:
        logging.exception(f"Error connecting to database '{db_path}' to get tables")
        return []
    finally:
        if con:
            con.close()


def find_latest_summary_file(output_dir):
    """Find the most recent summary file based on timestamp in filename.
    
    Args:
        output_dir: Directory where summary files are stored.
        
    Returns:
        Path to the latest summary file, or None if no summary files exist.
    """
    summary_pattern = os.path.join(output_dir, 'dump_scheduler_dbs_summary_*.json')
    summary_files = glob.glob(summary_pattern)
    
    if not summary_files:
        return None
    
    # Extract timestamp from filename and find the latest
    timestamp_pattern = re.compile(r'dump_scheduler_dbs_summary_(\d{8}_\d{6})\.json$')
    
    latest_file = None
    latest_timestamp = None
    
    for filepath in summary_files:
        filename = os.path.basename(filepath)
        match = timestamp_pattern.search(filename)
        if match:
            timestamp_str = match.group(1)
            try:
                timestamp = datetime.datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                if latest_timestamp is None or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    latest_file = filepath
            except ValueError:
                continue
    
    return latest_file


def check_previous_migration_status(output_dir):
    """Check if a previous successful migration exists.
    
    Args:
        output_dir: Directory where summary files are stored.
        
    Returns:
        A tuple of (should_run, reason, latest_summary_file)
        - should_run: True if migration should proceed, False to skip
    """
    latest_summary = find_latest_summary_file(output_dir)
    
    if latest_summary is None:
        return (True, "No previous migration summary found", None)
    
    try:
        with open(latest_summary, 'r', encoding='utf-8') as f:
            summary_data = json.load(f)
        
        overall = summary_data.get('overall', {})
        status = overall.get('status', 'unknown')
        
        if status == 'success':
            return (
                False,
                f"Previous migration completed successfully (summary: {latest_summary})",
                latest_summary
            )
        elif status in ('error', 'warning'):
            return (
                True,
                f"Previous migration had status '{status}', retrying (summary: {latest_summary})",
                latest_summary
            )
        else:
            return (
                True,
                f"Previous migration has unknown status '{status}', proceeding (summary: {latest_summary})",
                latest_summary
            )
    except (json.JSONDecodeError, IOError) as e:
        return (
            True,
            f"Could not read previous summary '{latest_summary}': {e}",
            latest_summary
        )


def main():
    # Statistics tracking
    stats_dict = {}

    arg_parser = argparse.ArgumentParser(
        description="Dump all tables from SQLite databases in a directory to JSON files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    arg_parser.add_argument(
        "db_dir",
        help="Directory containing the SQLite database files."
    )
    arg_parser.add_argument(
        "--output-dir",
        default="db_dumps",
        help="Directory to save the JSON dump files."
    )
    arg_parser.add_argument(
        "--force",
        action="store_true",
        help="Force migration even if a previous successful migration exists."
    )
    args = arg_parser.parse_args()
    db_dir = args.db_dir
    output_dir = args.output_dir

    # Create output directory first (needed to check for previous summary)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Check if migration should run based on previous summary BEFORE creating log file
    should_run, reason, last_summary = check_previous_migration_status(output_dir)
    
    if not should_run and not args.force:
        # Just print to console - don't create log file for skipped migrations
        print(f"[Migration Check] {reason}")
        print("[Migration Check] Skipping migration - previous migration was successful.")
        print("[Migration Check] Use --force flag to run migration anyway.")
        return

    # Only create log file if we're actually going to do something
    log_file = os.path.join(output_dir, f'dump_scheduler_dbs_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )

    logging.info("Starting database dump process...")
    logging.info(reason)
    
    if not should_run and args.force:
        logging.info("Force flag set - proceeding with migration despite previous success.")
    if not os.path.isdir(db_dir):
        error_msg = f"Database directory not found at '{db_dir}'"
        logging.error(error_msg)
        # Write error summary
        summary_log_file = os.path.join(output_dir, f'dump_scheduler_dbs_summary_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        summary_data = {
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "main_log_file": log_file,
            "databases": {},
            "overall": {
                "total_success": 0,
                "total_errors": 0,
                "status": "error",
                "message": error_msg
            }
        }
        with open(summary_log_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Summary log written to: {summary_log_file}")
        return
    try:
        db_files = [f for f in os.listdir(db_dir) if f.endswith(('.sqlite', '.db'))]
    except OSError as e:
        error_msg = f"Could not read directory '{db_dir}': {str(e)}"
        logging.exception(error_msg)
        # Write error summary
        summary_log_file = os.path.join(output_dir, f'dump_scheduler_dbs_summary_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        summary_data = {
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "main_log_file": log_file,
            "databases": {},
            "overall": {
                "total_success": 0,
                "total_errors": 0,
                "status": "error",
                "message": error_msg
            }
        }
        with open(summary_log_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Summary log written to: {summary_log_file}")
        return
    if not db_files:
        warning_msg = f"No SQLite databases found in '{db_dir}'"
        logging.warning(warning_msg)
        # Write warning summary
        summary_log_file = os.path.join(output_dir, f'dump_scheduler_dbs_summary_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        summary_data = {
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "main_log_file": log_file,
            "databases": {},
            "overall": {
                "total_success": 0,
                "total_errors": 0,
                "status": "warning",
                "message": warning_msg
            }
        }
        with open(summary_log_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Summary log written to: {summary_log_file}")
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info(f"Created temporary directory for database copies: {temp_dir}")
        for db_file in db_files:
            source_db_path = os.path.join(db_dir, db_file)
            temp_db_path = os.path.join(temp_dir, db_file)
            try:
                logging.info(f"Copying '{source_db_path}' to temporary location...")
                shutil.copy2(source_db_path, temp_db_path)
                logging.info(f"Processing temporary database: {temp_db_path}")
                tables = get_tables(temp_db_path)
                if tables:
                    logging.info(f"Found tables: {', '.join(tables)}")
                    db_name = Path(temp_db_path).stem
                    stats_dict[db_name] = {}

                    for table in tables:
                        success, errors = dump_table_to_json(temp_db_path, table, output_dir)
                        stats_dict[db_name][table] = (success, errors)

                    success, errors = dump_apscheduler_jobs_to_dict(temp_db_path, output_dir)
                    stats_dict[db_name]['apscheduler_jobs'] = (success, errors)
                else:
                    logging.warning(f"No tables found in '{temp_db_path}'")
            except Exception as e:
                error_msg = f"Error processing file '{source_db_path}': {str(e)}"
                logging.exception(error_msg)
                # Don't track corrupted databases in summary - just log the error
    # Write summary log as JSON
    summary_log_file = os.path.join(output_dir, f'dump_scheduler_dbs_summary_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

    summary_data = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "main_log_file": log_file,
        "databases": {},
        "overall": {
            "total_success": 0,
            "total_errors": 0
        }
    }

    for db_name, tables in stats_dict.items():
        db_summary = {
            "tables": {},
            "total_success": 0,
            "total_errors": 0
        }

        for table_name, (success, errors) in tables.items():
            if errors > 0 and success == 0:
                status = "error"
            elif errors > 0:
                status = "warning"
            else:
                status = "success"
            db_summary["tables"][table_name] = {
                "success": success,
                "errors": errors,
                "status": status
            }
            db_summary["total_success"] += success
            db_summary["total_errors"] += errors

        summary_data["databases"][db_name] = db_summary
        summary_data["overall"]["total_success"] += db_summary["total_success"]
        summary_data["overall"]["total_errors"] += db_summary["total_errors"]

    # Set overall status based on errors
    total_errors = summary_data["overall"]["total_errors"]
    total_success = summary_data["overall"]["total_success"]
    if total_errors > 0 and total_success == 0:
        summary_data["overall"]["status"] = "error"
    elif total_errors > 0:
        summary_data["overall"]["status"] = "warning"
    else:
        summary_data["overall"]["status"] = "success"
    
    with open(summary_log_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Summary log written to: {summary_log_file}")
    logging.info("Dump process finished. Temporary files have been removed.")


if __name__ == "__main__":
    main()