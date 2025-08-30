import streamlit as st
import subprocess
import sys
import os
from pathlib import Path
import time
import webbrowser
import json
from datetime import datetime, timedelta, time as dt_time
import threading
import schedule
import pickle
import tempfile
import json

# Page configuration
st.set_page_config(
    page_title="HR Attrition Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Local storage functions
def save_data_to_file():
    """Save email recipients and schedule config to local JSON file"""
    data = {
        'email_recipients': st.session_state.email_recipients,
        'scheduled_reports': st.session_state.scheduled_reports,
        'schedule_enabled': st.session_state.schedule_enabled,
        'default_report_type': st.session_state.get('default_report_type', 'Summary'),
        'scheduled_pipelines': st.session_state.scheduled_pipelines,
        'pipeline_schedule_enabled': st.session_state.pipeline_schedule_enabled,
        'last_updated': datetime.now().isoformat()
    }
    try:
        with open('dashboard_config.json', 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving config: {e}")

def load_data_from_file():
    """Load email recipients and schedule config from local JSON file"""
    try:
        if os.path.exists('dashboard_config.json'):
            with open('dashboard_config.json', 'r') as f:
                data = json.load(f)
            return data
    except Exception as e:
        print(f"Error loading config: {e}")
    return {
        'email_recipients': [],
        'scheduled_reports': [],
        'schedule_enabled': False,
        'default_report_type': 'Summary',
        'scheduled_pipelines': [],
        'pipeline_schedule_enabled': False
    }

# Initialize session state with saved data
saved_data = load_data_from_file()

if 'execution_history' not in st.session_state:
    st.session_state.execution_history = []
if 'email_recipients' not in st.session_state:
    st.session_state.email_recipients = saved_data.get('email_recipients', [])
if 'scheduled_reports' not in st.session_state:
    st.session_state.scheduled_reports = saved_data.get('scheduled_reports', [])
if 'schedule_enabled' not in st.session_state:
    st.session_state.schedule_enabled = saved_data.get('schedule_enabled', False)
if 'schedule_thread' not in st.session_state:
    st.session_state.schedule_thread = None
if 'schedule_config' not in st.session_state:
    st.session_state.schedule_config = None
if 'default_report_type' not in st.session_state:
    st.session_state.default_report_type = saved_data.get('default_report_type', 'Summary')
if 'scheduled_pipelines' not in st.session_state:
    st.session_state.scheduled_pipelines = saved_data.get('scheduled_pipelines', [])
if 'pipeline_schedule_enabled' not in st.session_state:
    st.session_state.pipeline_schedule_enabled = saved_data.get('pipeline_schedule_enabled', False)
if 'pipeline_thread' not in st.session_state:
    st.session_state.pipeline_thread = None
if 'pipeline_config' not in st.session_state:
    st.session_state.pipeline_config = None

# Auto-restore schedule function - will be called after functions are defined
def restore_schedule():
    """Restore schedule from saved configuration"""
    if st.session_state.schedule_enabled and st.session_state.scheduled_reports and st.session_state.email_recipients:
        try:
            # Check if we already have an active schedule
            if (st.session_state.schedule_thread and
                st.session_state.schedule_thread.is_alive()):
                return

            # Restore the last schedule
            last_schedule = st.session_state.scheduled_reports[-1]
            schedule_params = {
                'time': last_schedule['time'],
                'frequency': last_schedule['frequency'],
                'recipients': st.session_state.email_recipients
            }
            if last_schedule['frequency'] == "Weekly" and 'day_of_week' in last_schedule:
                schedule_params['day_of_week'] = last_schedule['day_of_week']

            # Restart the schedule
            if setup_schedule_enhanced(schedule_params):
                print(f"Schedule restored: {last_schedule['frequency']} at {last_schedule['time']}")
            else:
                st.session_state.schedule_enabled = False
                save_data_to_file()
        except Exception as e:
            print(f"Error restoring schedule: {e}")
            st.session_state.schedule_enabled = False
            save_data_to_file()

def restore_pipeline_schedule():
    """Restore pipeline schedule from saved configuration"""
    if st.session_state.pipeline_schedule_enabled and st.session_state.scheduled_pipelines:
        try:
            # Check if we already have an active pipeline schedule
            if (st.session_state.pipeline_thread and
                st.session_state.pipeline_thread.is_alive()):
                return

            # Restore the last pipeline schedule
            last_schedule = st.session_state.scheduled_pipelines[-1]
            schedule_params = {
                'time': last_schedule['time'],
                'frequency': last_schedule['frequency'],
                'pipeline_type': 'complete'
            }
            if last_schedule['frequency'] == "Weekly" and 'day_of_week' in last_schedule:
                schedule_params['day_of_week'] = last_schedule['day_of_week']

            # Restart the pipeline schedule
            if setup_pipeline_schedule_enhanced(schedule_params):
                print(f"Pipeline schedule restored: {last_schedule['frequency']} at {last_schedule['time']}")
            else:
                st.session_state.pipeline_schedule_enabled = False
                save_data_to_file()
        except Exception as e:
            print(f"Error restoring pipeline schedule: {e}")
            st.session_state.pipeline_schedule_enabled = False
            save_data_to_file()

# Professional CSS Styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global Styles */
    .main-header {
        font-family: 'Inter', sans-serif;
        text-align: center;
        color: #2c3e50;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 2rem;
    }

    /* Script Card Text Styling */
    .script-card, .pipeline-card, .dashboard-card {
        color: #2c3e50 !important;
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        margin-bottom: 1rem;
    }

    .script-card h3, .pipeline-card h3, .dashboard-card h3 {
        font-size: 1.2rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
        color: #2c3e50 !important;
    }

    .script-card p, .pipeline-card p, .dashboard-card p {
        font-size: 0.9rem;
        color: #555 !important;
        margin: 0.2rem 0;
    }

    /* Button Styles */
    .stButton > button {
        background: #2c3e50;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-weight: 500;
        font-size: 0.95rem;
        transition: all 0.2s ease;
        box-shadow: 0 3px 8px rgba(0,0,0,0.1);
        width: 100%;
    }

    .stButton > button:hover {
        transform: translateY(-1px);
        background: #34495e;
    }

    /* Alerts */
    .success-alert {
        background: #eafaf1;
        color: #2d7a46;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border: 1px solid #b8e0c2;
    }

    .error-alert {
        background: #fdecea;
        color: #a94442;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border: 1px solid #f5c6cb;
    }

    .info-alert {
        background: #e8f4fd;
        color: #31708f;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border: 1px solid #bcdff1;
    }

    /* Metric Cards */
    .metric-card {
        background: white;
        padding: 1.2rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        margin: 0.5rem 0;
    }

    .metric-number {
        font-size: 2rem;
        font-weight: 600;
        color: #2c3e50;
        margin: 0;
    }

    .metric-label {
        font-size: 0.85rem;
        color: #6c757d;
        margin-top: 0.3rem;
    }

    /* Sidebar */
    .css-1d391kg {
        background: #f8f9fa;
    }

    /* Hide Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

def log_execution(script_name, success, duration, output=None, error=None):
    """Log script execution results"""
    execution_record = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'script': script_name,
        'success': success,
        'duration': duration,
        'output': output,
        'error': error
    }
    st.session_state.execution_history.insert(0, execution_record)
    if len(st.session_state.execution_history) > 10:
        st.session_state.execution_history = st.session_state.execution_history[:10]

def run_script_with_progress(script_path, script_name):
    """Run a Python script with simulated progress"""
    start_time = time.time()
    try:
        project_root = Path(__file__).resolve().parent
        env = dict(os.environ)
        env['PYTHONPATH'] = str(project_root)

        progress_container = st.empty()
        with progress_container.container():
            st.markdown("Running script...")
            progress_bar = st.progress(0)
            status_text = st.empty()

        for i in range(100):
            progress_bar.progress((i + 1) / 100)
            if i < 30:
                status_text.text("Initializing...")
            elif i < 60:
                status_text.text("Processing...")
            elif i < 90:
                status_text.text("Analyzing...")
            else:
                status_text.text("Finalizing...")
            time.sleep(0.02)

        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
            cwd=str(project_root)
        )

        progress_container.empty()
        duration = time.time() - start_time
        success = result.returncode == 0
        log_execution(script_name, success, duration, result.stdout, result.stderr)
        return success, result.stdout, result.stderr, duration

    except Exception as e:
        duration = time.time() - start_time
        log_execution(script_name, False, duration, None, str(e))
        return False, "", str(e), duration

def run_report_generation(send_email=False, custom_recipients=None, report_type="Full"):
    """Run report generation with optional email sending and report type selection"""
    start_time = time.time()
    try:
        project_root = Path(__file__).resolve().parent
        env = dict(os.environ)
        env['PYTHONPATH'] = str(project_root)

        report_script = project_root / "etl" / "Email_Report.py"

        if not report_script.exists():
            return False, "", "Email_Report.py not found", 0

        # If we don't want to send email, we'll modify the environment to skip email
        if not send_email:
            env['SKIP_EMAIL'] = 'true'
        elif custom_recipients:
            env['CUSTOM_EMAIL_RECIPIENTS'] = ','.join(custom_recipients)

        # Set report type
        env['REPORT_TYPE'] = report_type

        result = subprocess.run(
            [sys.executable, str(report_script)],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
            cwd=str(project_root)
        )

        duration = time.time() - start_time
        success = result.returncode == 0
        script_name = f"{report_type} Report" + (" + Email" if send_email else "")
        log_execution(script_name, success, duration, result.stdout, result.stderr)
        return success, result.stdout, result.stderr, duration

    except Exception as e:
        duration = time.time() - start_time
        script_name = f"{report_type} Report" + (" + Email" if send_email else "")
        log_execution(script_name, False, duration, None, str(e))
        return False, "", str(e), duration

def create_scheduled_report_job(recipients_list, schedule_config_path):
    """Create a scheduled report job that can run independently"""
    def job():
        try:
            # Load current config
            with open(schedule_config_path, 'rb') as f:
                config = pickle.load(f)

            if not config.get('enabled', False):
                return

            print(f"[{datetime.now()}] Running scheduled report job...")

            # Run report generation
            project_root = Path(__file__).resolve().parent
            env = dict(os.environ)
            env['PYTHONPATH'] = str(project_root)
            env['CUSTOM_EMAIL_RECIPIENTS'] = ','.join(recipients_list)
            env['REPORT_TYPE'] = config.get('report_type', 'Summary')

            report_script = project_root / "etl" / "Email_Report.py"

            result = subprocess.run(
                [sys.executable, str(report_script)],
                capture_output=True,
                text=True,
                timeout=300,
                env=env,
                cwd=str(project_root)
            )

            success = result.returncode == 0
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Save execution log to file for later retrieval
            log_file = project_root / "scheduled_reports.log"
            with open(log_file, 'a') as f:
                f.write(f"{timestamp} | {'SUCCESS' if success else 'FAILED'} | Recipients: {len(recipients_list)}\n")
                if result.stdout:
                    f.write(f"OUTPUT: {result.stdout}\n")
                if result.stderr:
                    f.write(f"ERROR: {result.stderr}\n")
                f.write("-" * 80 + "\n")

            print(f"[{datetime.now()}] Scheduled report {'completed successfully' if success else 'failed'}")

        except Exception as e:
            print(f"[{datetime.now()}] Scheduled report failed: {str(e)}")

    return job

def create_scheduled_pipeline_job(schedule_config_path):
    """Create a scheduled pipeline job that can run independently"""
    def job():
        try:
            # Load current config
            with open(schedule_config_path, 'rb') as f:
                config = pickle.load(f)

            if not config.get('enabled', False):
                return

            print(f"[{datetime.now()}] Running scheduled pipeline job...")

            # Run complete pipeline
            project_root = Path(__file__).resolve().parent
            env = dict(os.environ)
            env['PYTHONPATH'] = str(project_root)

            main_script = project_root / "main.py"

            result = subprocess.run(
                [sys.executable, str(main_script)],
                capture_output=True,
                text=True,
                timeout=1200,  # 20 minutes timeout for pipeline
                env=env,
                cwd=str(project_root)
            )

            success = result.returncode == 0
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Save execution log to file for later retrieval
            log_file = project_root / "scheduled_pipelines.log"
            with open(log_file, 'a') as f:
                f.write(f"{timestamp} | {'SUCCESS' if success else 'FAILED'} | Complete ETL Pipeline\n")
                if result.stdout:
                    f.write(f"OUTPUT: {result.stdout}\n")
                if result.stderr:
                    f.write(f"ERROR: {result.stderr}\n")
                f.write("-" * 80 + "\n")

            print(f"[{datetime.now()}] Scheduled pipeline {'completed successfully' if success else 'failed'}")

        except Exception as e:
            print(f"[{datetime.now()}] Scheduled pipeline failed: {str(e)}")

    return job

def schedule_worker(schedule_config_path):
    """Background worker to run scheduled jobs"""
    while True:
        try:
            # Check if scheduling is still enabled
            if os.path.exists(schedule_config_path):
                with open(schedule_config_path, 'rb') as f:
                    config = pickle.load(f)
                if not config.get('enabled', False):
                    break
            else:
                break

            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Schedule worker error: {e}")
            break

def pipeline_schedule_worker(schedule_config_path):
    """Background worker to run scheduled pipeline jobs"""
    while True:
        try:
            # Check if pipeline scheduling is still enabled
            if os.path.exists(schedule_config_path):
                with open(schedule_config_path, 'rb') as f:
                    config = pickle.load(f)
                if not config.get('enabled', False):
                    break
            else:
                break

            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Pipeline schedule worker error: {e}")
            break

def setup_schedule_enhanced(params):
    """Enhanced setup for scheduled reports with custom parameters"""
    try:
        # Clear any existing schedules
        schedule.clear()

        # Stop existing thread if running
        if (st.session_state.schedule_thread and
            st.session_state.schedule_thread.is_alive()):
            # Mark as disabled to stop the worker
            if st.session_state.schedule_config:
                config = {'enabled': False}
                with open(st.session_state.schedule_config, 'wb') as f:
                    pickle.dump(config, f)
            st.session_state.schedule_thread.join(timeout=2)

        # Create persistent config file instead of temp file
        project_root = Path(__file__).resolve().parent
        schedule_config_path = project_root / "schedule_config.pkl"

        # Save config with all parameters
        config = {
            'enabled': True,
            **params  # Unpack all parameters
        }
        with open(schedule_config_path, 'wb') as f:
            pickle.dump(config, f)

        # Create the job
        job = create_scheduled_report_job(params['recipients'], schedule_config_path)

        # Schedule the job based on frequency and parameters
        frequency = params['frequency']
        schedule_time = params['time']

        if frequency == "Daily":
            schedule.every().day.at(schedule_time).do(job)
        elif frequency == "Weekly":
            day_map = {
                "Monday": schedule.every().monday,
                "Tuesday": schedule.every().tuesday,
                "Wednesday": schedule.every().wednesday,
                "Thursday": schedule.every().thursday,
                "Friday": schedule.every().friday,
                "Saturday": schedule.every().saturday,
                "Sunday": schedule.every().sunday
            }
            day_of_week = params.get('day_of_week', 'Monday')
            day_map[day_of_week].at(schedule_time).do(job)
        elif frequency == "Monthly":
            # For monthly, we'll use a custom approach since schedule library doesn't support specific days
            schedule.every(30).days.at(schedule_time).do(job)  # Approximate monthly
        elif frequency == "Custom Interval":
            interval_type = params.get('interval_type', 'Minutes')
            interval_value = params.get('interval_value', 30)

            if interval_type == "Minutes":
                schedule.every(interval_value).minutes.do(job)
            elif interval_type == "Hours":
                schedule.every(interval_value).hours.do(job)
            else:  # Days
                schedule.every(interval_value).days.at(schedule_time).do(job)

        # Start background thread
        thread = threading.Thread(target=schedule_worker, args=(schedule_config_path,), daemon=True)
        thread.start()

        # Store in session state
        st.session_state.schedule_thread = thread
        st.session_state.schedule_config = schedule_config_path

        print(f"Schedule setup: {frequency} at {schedule_time}")
        return True

    except Exception as e:
        print(f"Setup schedule error: {e}")
        return False

def setup_pipeline_schedule_enhanced(params):
    """Enhanced setup for scheduled pipeline with custom parameters"""
    try:
        # Clear any existing pipeline schedules
        schedule.clear()

        # Stop existing pipeline thread if running
        if (st.session_state.pipeline_thread and
            st.session_state.pipeline_thread.is_alive()):
            # Mark as disabled to stop the worker
            if st.session_state.pipeline_config:
                config = {'enabled': False}
                with open(st.session_state.pipeline_config, 'wb') as f:
                    pickle.dump(config, f)
            st.session_state.pipeline_thread.join(timeout=2)

        # Create persistent config file for pipeline
        project_root = Path(__file__).resolve().parent
        schedule_config_path = project_root / "pipeline_schedule_config.pkl"

        # Save config with all parameters
        config = {
            'enabled': True,
            **params  # Unpack all parameters
        }
        with open(schedule_config_path, 'wb') as f:
            pickle.dump(config, f)

        # Create the pipeline job
        job = create_scheduled_pipeline_job(schedule_config_path)

        # Schedule the job based on frequency and parameters
        frequency = params['frequency']
        schedule_time = params['time']

        if frequency == "Daily":
            schedule.every().day.at(schedule_time).do(job)
        elif frequency == "Weekly":
            day_map = {
                "Monday": schedule.every().monday,
                "Tuesday": schedule.every().tuesday,
                "Wednesday": schedule.every().wednesday,
                "Thursday": schedule.every().thursday,
                "Friday": schedule.every().friday,
                "Saturday": schedule.every().saturday,
                "Sunday": schedule.every().sunday
            }
            day_of_week = params.get('day_of_week', 'Monday')
            day_map[day_of_week].at(schedule_time).do(job)

        # Start background thread
        thread = threading.Thread(target=pipeline_schedule_worker, args=(schedule_config_path,), daemon=True)
        thread.start()

        # Store in session state
        st.session_state.pipeline_thread = thread
        st.session_state.pipeline_config = schedule_config_path

        print(f"Pipeline schedule setup: {frequency} at {schedule_time}")
        return True

    except Exception as e:
        print(f"Setup pipeline schedule error: {e}")
        return False

def setup_schedule(schedule_time, frequency, recipients):
    """Legacy function for backward compatibility"""
    params = {
        'time': schedule_time,
        'frequency': frequency,
        'recipients': recipients
    }
    return setup_schedule_enhanced(params)

def stop_schedule():
    """Stop scheduled reports"""
    try:
        schedule.clear()

        if st.session_state.schedule_config and os.path.exists(st.session_state.schedule_config):
            # Mark as disabled
            config = {'enabled': False}
            with open(st.session_state.schedule_config, 'wb') as f:
                pickle.dump(config, f)

        if (st.session_state.schedule_thread and
            st.session_state.schedule_thread.is_alive()):
            st.session_state.schedule_thread.join(timeout=2)
            st.session_state.schedule_thread = None

        print("Schedule stopped")

    except Exception as e:
        print(f"Stop schedule error: {e}")

def stop_pipeline_schedule():
    """Stop scheduled pipeline jobs"""
    try:
        schedule.clear()

        if st.session_state.pipeline_config and os.path.exists(st.session_state.pipeline_config):
            # Mark as disabled
            config = {'enabled': False}
            with open(st.session_state.pipeline_config, 'wb') as f:
                pickle.dump(config, f)

        if (st.session_state.pipeline_thread and
            st.session_state.pipeline_thread.is_alive()):
            st.session_state.pipeline_thread.join(timeout=2)
            st.session_state.pipeline_thread = None

        print("Pipeline schedule stopped")

    except Exception as e:
        print(f"Stop pipeline schedule error: {e}")

def get_scheduled_logs():
    """Get logs from scheduled reports"""
    try:
        log_file = Path(__file__).resolve().parent / "scheduled_reports.log"
        if log_file.exists():
            with open(log_file, 'r') as f:
                return f.read()
    except Exception as e:
        print(f"Error reading scheduled logs: {e}")
    return "No scheduled report logs found."

def get_scheduled_pipeline_logs():
    """Get logs from scheduled pipeline jobs"""
    try:
        log_file = Path(__file__).resolve().parent / "scheduled_pipelines.log"
        if log_file.exists():
            with open(log_file, 'r') as f:
                return f.read()
    except Exception as e:
        print(f"Error reading scheduled pipeline logs: {e}")
    return "No scheduled pipeline logs found."

def display_execution_metrics():
    if not st.session_state.execution_history:
        return
    st.markdown("### Execution Metrics")

    col1, col2, col3, col4 = st.columns(4)
    total_executions = len(st.session_state.execution_history)
    successful_executions = sum(1 for exec in st.session_state.execution_history if exec['success'])
    avg_duration = sum(exec['duration'] for exec in st.session_state.execution_history) / total_executions
    success_rate = (successful_executions / total_executions) * 100

    with col1:
        st.markdown(f"""<div class="metric-card"><div class="metric-number">{total_executions}</div><div class="metric-label">Total Runs</div></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-card"><div class="metric-number">{successful_executions}</div><div class="metric-label">Successful</div></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-card"><div class="metric-number">{avg_duration:.1f}s</div><div class="metric-label">Avg Duration</div></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="metric-card"><div class="metric-number">{success_rate:.0f}%</div><div class="metric-label">Success Rate</div></div>""", unsafe_allow_html=True)

def display_execution_history():
    if not st.session_state.execution_history:
        return
    st.markdown("### Recent Executions")
    for exec in st.session_state.execution_history[:5]:
        status_icon = "Success" if exec['success'] else "Failed"
        status_class = "success-alert" if exec['success'] else "error-alert"
        with st.expander(f"{status_icon} - {exec['script']} - {exec['timestamp']} ({exec['duration']:.1f}s)"):
            if exec['output']:
                st.code(exec['output'], language='text')
            if exec['error']:
                st.error(exec['error'])

def main():
    # Restore schedules on app start (after all functions are defined)
    restore_schedule()
    restore_pipeline_schedule()

    st.markdown('<h1 class="main-header">HR Attrition Intelligence Dashboard</h1>', unsafe_allow_html=True)

    project_root = Path(__file__).resolve().parent
    etl_dir = project_root / "etl"

    scripts_info = {
        "reviews_scraper.py": {"display_name": "Reviews Scraper", "description": "Scrapes employee reviews from external sources.", "category": "Data Collection", "estimated_time": "2-3 minutes"},
        "internal_hrms_data_generator.py": {"display_name": "HRMS Data Generator", "description": "Generates synthetic HRMS data with realistic patterns.", "category": "Data Generation", "estimated_time": "1-2 minutes"},
        "data_merger.py": {"display_name": "Data Merger", "description": "Merges scraped reviews with HRMS data.", "category": "Data Processing", "estimated_time": "3-4 minutes"},
        "push.py": {"display_name": "Data Push", "description": "Pushes processed data to the database with validation.", "category": "Data Storage", "estimated_time": "1-2 minutes"}
    }

    # Sidebar
    with st.sidebar:
        st.markdown("## Control Panel")
        if st.button("Refresh Status"):
            st.rerun()
        if st.button("Clear History"):
            st.session_state.execution_history = []
            st.success("History cleared")
            time.sleep(1)
            st.rerun()
        st.markdown("---")
        st.markdown("### System Status")
        all_scripts_exist = True
        for script_file in scripts_info.keys():
            script_path = etl_dir / script_file
            status = "Available" if script_path.exists() else "Missing"
            st.markdown(f"{script_file}: {status}")
            if not script_path.exists():
                all_scripts_exist = False
        main_script = project_root / "main.py"
        st.markdown(f"main.py: {'Available' if main_script.exists() else 'Missing'}")
        st.success("All systems operational" if all_scripts_exist else "Some components missing")

        st.markdown("---")
        st.markdown("### Persistence Status")

        # Show email recipients status
        if st.session_state.email_recipients:
            st.success(f"📧 {len(st.session_state.email_recipients)} recipients saved")
        else:
            st.info("📧 No recipients configured")

        # Show schedule status
        if st.session_state.schedule_enabled and st.session_state.scheduled_reports:
            schedule_info = st.session_state.scheduled_reports[-1]
            schedule_desc = f"{schedule_info['frequency']} at {schedule_info['time']}"
            report_type = schedule_info.get('report_type', 'Summary')
            if (st.session_state.schedule_thread and st.session_state.schedule_thread.is_alive()):
                st.success(f"🟢 Schedule active: {schedule_desc} ({report_type})")
            else:
                st.warning(f"🟡 Schedule saved: {schedule_desc} ({report_type})")
        else:
            st.info("📅 No schedule configured")

        # Show pipeline schedule status
        if st.session_state.pipeline_schedule_enabled and st.session_state.scheduled_pipelines:
            pipeline_info = st.session_state.scheduled_pipelines[-1]
            pipeline_desc = f"{pipeline_info['frequency']} at {pipeline_info['time']}"
            if (st.session_state.pipeline_thread and st.session_state.pipeline_thread.is_alive()):
                st.success(f"🟢 Pipeline schedule active: {pipeline_desc}")
            else:
                st.warning(f"🟡 Pipeline schedule saved: {pipeline_desc}")
        else:
            st.info("⚙️ No pipeline schedule configured")

        # Show default report type
        st.info(f"📄 Default report: {st.session_state.default_report_type}")

        # Show config file status
        config_exists = os.path.exists('dashboard_config.json')
        st.info(f"💾 Config file: {'Saved' if config_exists else 'None'}")

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Scripts", "Pipeline", "Reports", "Analytics", "Monitoring"])

    with tab1:
        st.markdown("## Individual Script Execution")
        cols = st.columns(2)
        for i, (script_file, info) in enumerate(scripts_info.items()):
            with cols[i % 2]:
                st.markdown(f"""<div class="script-card"><h3>{info['display_name']}</h3><p><strong>Category:</strong> {info['category']}</p><p><strong>Duration:</strong> ~{info['estimated_time']}</p><p>{info['description']}</p></div>""", unsafe_allow_html=True)
                if st.button(f"Run {info['display_name']}", key=f"run_{script_file}"):
                    if (etl_dir / script_file).exists():
                        success, stdout, stderr, duration = run_script_with_progress(etl_dir / script_file, info['display_name'])
                        if success:
                            st.markdown(f'<div class="success-alert">{info["display_name"]} completed successfully in {duration:.1f}s.</div>', unsafe_allow_html=True)
                            if stdout: st.expander("View Output").code(stdout, language='text')
                        else:
                            st.markdown(f'<div class="error-alert">{info["display_name"]} failed after {duration:.1f}s.</div>', unsafe_allow_html=True)
                            if stderr: st.expander("Error Details").code(stderr, language='text')

    with tab2:
        st.markdown("## Complete ETL Pipeline")
        st.markdown("Execute and schedule the complete ETL pipeline: Reviews Scraper → HRMS Generator → Data Merger → Data Push")

        # Pipeline Configuration Section
        with st.expander("⚙️ Pipeline Schedule Configuration", expanded=False):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Schedule Pipeline Execution**")
                pipeline_schedule_enabled = st.checkbox("Enable Pipeline Scheduling", value=st.session_state.pipeline_schedule_enabled)

                if pipeline_schedule_enabled:
                    pipeline_frequency = st.selectbox("Frequency:", ["Daily", "Weekly"], key="pipeline_freq")

                    pipeline_preset_times = {
                        "06:00 (Early Morning)": "06:00",
                        "22:00 (Late Night)": "22:00",
                        "02:00 (Off Hours)": "02:00",
                        "Custom": None
                    }

                    pipeline_selected_preset = st.selectbox("Time:", list(pipeline_preset_times.keys()), key="pipeline_time")

                    if pipeline_selected_preset != "Custom":
                        pipeline_time_str = pipeline_preset_times[pipeline_selected_preset]
                    else:
                        pipeline_time_str = st.text_input("Custom Time (HH:MM):", value="06:00", placeholder="06:00", key="pipeline_custom_time")

                    # Validate time
                    try:
                        time_parts = pipeline_time_str.split(":")
                        hours, minutes = int(time_parts[0]), int(time_parts[1])
                        if 0 <= hours <= 23 and 0 <= minutes <= 59:
                            pipeline_schedule_time = dt_time(hours, minutes)
                        else:
                            st.error("Invalid time format")
                            pipeline_schedule_time = dt_time(6, 0)
                    except:
                        st.error("Use HH:MM format")
                        pipeline_schedule_time = dt_time(6, 0)

                    if pipeline_frequency == "Weekly":
                        pipeline_day_of_week = st.selectbox("Day:", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"], key="pipeline_day")

                    col_setup, col_clear = st.columns(2)

                    with col_setup:
                        if st.button("Setup Pipeline Schedule"):
                            pipeline_params = {
                                'time': pipeline_schedule_time.strftime("%H:%M"),
                                'frequency': pipeline_frequency,
                                'pipeline_type': 'complete'
                            }
                            if pipeline_frequency == "Weekly":
                                pipeline_params['day_of_week'] = pipeline_day_of_week

                            if setup_pipeline_schedule_enhanced(pipeline_params):
                                st.session_state.pipeline_schedule_enabled = True
                                pipeline_record = {
                                    'frequency': pipeline_frequency,
                                    'time': pipeline_schedule_time.strftime("%H:%M"),
                                    'pipeline_type': 'complete'
                                }
                                if pipeline_frequency == "Weekly":
                                    pipeline_record['day_of_week'] = pipeline_day_of_week

                                st.session_state.scheduled_pipelines = [pipeline_record]
                                save_data_to_file()
                                st.success("Pipeline schedule set!")

                    with col_clear:
                        if st.button("Clear Pipeline Schedule"):
                            stop_pipeline_schedule()
                            st.session_state.pipeline_schedule_enabled = False
                            st.session_state.scheduled_pipelines = []
                            save_data_to_file()

            with col2:
                st.markdown("**Pipeline Schedule Info**")
                if st.session_state.scheduled_pipelines:
                    for sched in st.session_state.scheduled_pipelines:
                        schedule_desc = f"{sched['frequency']} at {sched['time']}"
                        if sched['frequency'] == "Weekly" and 'day_of_week' in sched:
                            schedule_desc = f"Weekly on {sched['day_of_week']} at {sched['time']}"

                        status_icon = "🟢" if (st.session_state.pipeline_thread and
                                             st.session_state.pipeline_thread.is_alive()) else "🔴"
                        st.info(f"{status_icon} {schedule_desc} → Complete ETL Pipeline")

                # Pipeline logs
                if st.session_state.scheduled_pipelines and st.button("📄 View Pipeline Logs"):
                    logs = get_scheduled_pipeline_logs()
                    st.text_area("Scheduled Pipeline Logs:", value=logs, height=200)

        st.markdown("---")

        # Manual Pipeline Execution
        st.markdown("**Manual Pipeline Execution**")
        st.markdown("""<div class="pipeline-card"><h3>Full Data Pipeline</h3><p>Executes all ETL steps in sequence: Reviews Scraper → HRMS Generator → Data Merger → Data Push</p><p><strong>Estimated Time:</strong> 7-11 minutes</p></div>""", unsafe_allow_html=True)
        if st.button("Execute Complete Pipeline", key="full_pipeline"):
            main_script = project_root / "main.py"
            if main_script.exists():
                success, stdout, stderr, duration = run_script_with_progress(main_script, "Complete Pipeline")
                if success:
                    st.markdown(f'<div class="success-alert">Pipeline executed successfully in {duration:.1f}s.</div>', unsafe_allow_html=True)
                    if stdout: st.expander("View Complete Output").code(stdout, language='text')
                else:
                    st.markdown(f'<div class="error-alert">Pipeline execution failed after {duration:.1f}s.</div>', unsafe_allow_html=True)
                    if stderr: st.expander("Error Details").code(stderr, language='text')
            else:
                st.error("main.py not found")

    with tab3:
        st.markdown("## HR Analytics Reports")
        st.markdown("Generate comprehensive HR analytics reports with charts and insights.")

        # Configuration Section
        with st.expander("⚙️ Configuration", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Email Recipients**")
                new_email = st.text_input("Add Email:", placeholder="example@company.com")
                if st.button("Add"):
                    if new_email and new_email not in st.session_state.email_recipients:
                        st.session_state.email_recipients.append(new_email)
                        save_data_to_file()
                        st.rerun()

                if st.session_state.email_recipients:
                    for i, email in enumerate(st.session_state.email_recipients):
                        col_email, col_remove = st.columns([3, 1])
                        with col_email:
                            st.text(email)
                        with col_remove:
                            if st.button("×", key=f"remove_{i}"):
                                st.session_state.email_recipients.remove(email)
                                save_data_to_file()
                                st.rerun()

            with col2:
                st.markdown("**Schedule Reports**")
                schedule_enabled = st.checkbox("Enable Scheduling", value=st.session_state.schedule_enabled)

                if schedule_enabled:
                    frequency = st.selectbox("Frequency:", ["Daily", "Weekly"])

                    preset_times = {
                        "09:00 (Morning)": "09:00",
                        "12:00 (Lunch)": "12:00",
                        "17:00 (End of Day)": "17:00",
                        "Custom": None
                    }

                    selected_preset = st.selectbox("Time:", list(preset_times.keys()))

                    if selected_preset != "Custom":
                        time_str = preset_times[selected_preset]
                    else:
                        time_str = st.text_input("Custom Time (HH:MM):", value="11:24", placeholder="11:24")

                    # Validate time
                    try:
                        time_parts = time_str.split(":")
                        hours, minutes = int(time_parts[0]), int(time_parts[1])
                        if 0 <= hours <= 23 and 0 <= minutes <= 59:
                            schedule_time = dt_time(hours, minutes)
                        else:
                            st.error("Invalid time format")
                            schedule_time = dt_time(9, 0)
                    except:
                        st.error("Use HH:MM format")
                        schedule_time = dt_time(9, 0)

                    if frequency == "Weekly":
                        day_of_week = st.selectbox("Day:", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])

                    col_setup, col_clear = st.columns(2)

                    with col_setup:
                        if st.button("Setup"):
                            if st.session_state.email_recipients:
                                schedule_params = {
                                    'time': schedule_time.strftime("%H:%M"),
                                    'frequency': frequency,
                                    'recipients': st.session_state.email_recipients,
                                    'report_type': st.session_state.default_report_type
                                }
                                if frequency == "Weekly":
                                    schedule_params['day_of_week'] = day_of_week

                                if setup_schedule_enhanced(schedule_params):
                                    st.session_state.schedule_enabled = True
                                    schedule_record = {
                                        'frequency': frequency,
                                        'time': schedule_time.strftime("%H:%M"),
                                        'recipients': len(st.session_state.email_recipients),
                                        'report_type': st.session_state.default_report_type
                                    }
                                    if frequency == "Weekly":
                                        schedule_record['day_of_week'] = day_of_week

                                    st.session_state.scheduled_reports = [schedule_record]
                                    save_data_to_file()
                                    st.success("Schedule set!")
                            else:
                                st.error("Add recipients first")

                    with col_clear:
                        if st.button("Clear"):
                            stop_schedule()
                            st.session_state.schedule_enabled = False
                            st.session_state.scheduled_reports = []
                            save_data_to_file()

                # Show current schedule
                if st.session_state.scheduled_reports:
                    for sched in st.session_state.scheduled_reports:
                        schedule_desc = f"{sched['frequency']} at {sched['time']}"
                        if sched['frequency'] == "Weekly" and 'day_of_week' in sched:
                            schedule_desc = f"Weekly on {sched['day_of_week']} at {sched['time']}"

                        # Show schedule status
                        status_icon = "🟢" if (st.session_state.schedule_thread and
                                             st.session_state.schedule_thread.is_alive()) else "🔴"
                        st.info(f"{status_icon} {schedule_desc} → {sched['recipients']} recipients")

        st.markdown("---")

        # Report Type Selection
        st.markdown("**Report Type:**")
        col_type, col_default = st.columns([2, 1])

        with col_type:
            report_type = st.radio(
                "Choose report type:",
                ["Summary", "Full Report"],
                index=0 if st.session_state.default_report_type == "Summary" else 1,
                horizontal=True,
                help="Summary: Key metrics and charts only. Full Report: Complete analysis with all details."
            )

        with col_default:
            if st.button("Set as Default"):
                st.session_state.default_report_type = report_type
                save_data_to_file()
                st.success(f"Default: {report_type}")

        st.markdown("---")

        # Report Generation
        col1, col2 = st.columns(2)

        with col1:
            if st.button("📊 Generate Report"):
                with st.spinner(f"Generating {report_type.lower()}..."):
                    success, stdout, stderr, duration = run_report_generation(
                        send_email=False,
                        report_type=report_type
                    )
                    if success:
                        st.success(f"{report_type} generated in {duration:.1f}s")
                        # Check if PDF was created and offer download
                        pdf_path = project_root / "HR_Analytics_Report.pdf"
                        if pdf_path.exists():
                            with open(pdf_path, "rb") as pdf_file:
                                st.download_button(
                                    label="Download Report",
                                    data=pdf_file.read(),
                                    file_name=f"HR_{report_type.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                                    mime="application/pdf"
                                )
                    else:
                        st.error(f"Generation failed after {duration:.1f}s")

        with col2:
            if st.button("📧 Generate & Email"):
                if not st.session_state.email_recipients:
                    st.error("Add recipients first")
                else:
                    with st.spinner(f"Generating and emailing {report_type.lower()}..."):
                        success, stdout, stderr, duration = run_report_generation(
                            send_email=True,
                            custom_recipients=st.session_state.email_recipients,
                            report_type=report_type
                        )
                        if success:
                            st.success(f"{report_type} emailed to {len(st.session_state.email_recipients)} recipients in {duration:.1f}s")
                        else:
                            st.error(f"Failed after {duration:.1f}s")

    with tab4:
        st.markdown("## Analytics Dashboard")
        st.markdown("Interactive HR attrition analytics with real-time visualizations and insights.")

        # Dashboard controls
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            st.markdown("**Live Dashboard:**")

        with col2:
            height_option = st.selectbox("Height:", ["Standard (800px)", "Tall (1000px)", "Full (1200px)"], index=0)
            height_map = {"Standard (800px)": "800", "Tall (1000px)": "1000", "Full (1200px)": "1200"}
            iframe_height = height_map[height_option]

        with col3:
            if st.button("🔗 Open in New Tab"):
                looker_url = "https://lookerstudio.google.com/reporting/5c455533-2a58-4ada-9b71-edcb282d6fed/page/Da6UF"
                st.markdown(f'<script>window.open("{looker_url}", "_blank")</script>', unsafe_allow_html=True)

        st.markdown("---")

        # Embed Looker Studio dashboard
        embed_url = "https://lookerstudio.google.com/embed/reporting/5c455533-2a58-4ada-9b71-edcb282d6fed/page/Da6UF"

        # Create responsive iframe HTML
        iframe_html = f"""
        <div style="position: relative; width: 100%; overflow: hidden;">
            <iframe
                src="{embed_url}"
                width="100%"
                height="{iframe_height}"
                frameborder="0"
                style="border:0; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); display: block;"
                allowfullscreen
                loading="lazy">
            </iframe>
        </div>
        """

        # Display the embedded dashboard
        st.markdown(iframe_html, unsafe_allow_html=True)

        # Additional info
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info("📊 **Real-time Data**: Dashboard updates automatically from SupabaseData")
        with col2:
            st.info("🔄 **Interactive**: Click, filter, and explore the visualizations")
        with col3:
            st.info("📱 **Responsive**: Optimized for different screen sizes")

    with tab5:
        st.markdown("## Monitoring & History")
        display_execution_metrics()
        st.markdown("---")
        display_execution_history()
        if st.session_state.execution_history:
            if st.button("Export Execution Log"):
                log_data = json.dumps(st.session_state.execution_history, indent=2)
                st.download_button("Download Log JSON", log_data, file_name=f"execution_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", mime="application/json")
        else:
            st.markdown('<div class="info-alert">No execution history available. Run scripts to see monitoring data.</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""<div style="text-align:center; color:#64748b; font-size:0.9rem; margin-top:2rem;"><p>HR Attrition Intelligence Dashboard v2.0 | Built with Streamlit</p></div>""", unsafe_allow_html=True)

def cleanup_on_exit():
    """Cleanup function to stop scheduled tasks on app exit but keep config"""
    try:
        # Only stop the thread, keep the configuration for next startup
        if (st.session_state.schedule_thread and
            st.session_state.schedule_thread.is_alive()):
            st.session_state.schedule_thread.join(timeout=2)
        schedule.clear()
        print("Cleaned up scheduled tasks")
    except Exception as e:
        print(f"Cleanup error: {e}")

# Register cleanup function
import atexit
atexit.register(cleanup_on_exit)

if __name__ == "__main__":
    main()
