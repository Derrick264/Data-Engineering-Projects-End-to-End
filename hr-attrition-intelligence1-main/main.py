import subprocess
import sys
from pathlib import Path

def run_script(script_path):
    print(f"\n🔹 Running {script_path.name} ...")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"❌ ERROR running {script_path.name}:\n{result.stderr}")
        sys.exit(result.returncode)
    else:
        print(f"✅ {script_path.name} completed.")

if __name__ == "__main__":
    # Set up paths (update these if your structure changes)
    project_root = Path(__file__).resolve().parent
    etl_dir = project_root / "etl"

    scripts = [
        etl_dir / "reviews_scraper.py",
        etl_dir / "internal_hrms_data_generator.py",
        etl_dir / "data_merger.py",
        etl_dir / "push.py",

    ]

    for script in scripts:
        if not script.exists():
            print(f"❌ Script not found: {script}")
            sys.exit(1)
        run_script(script)
#updated
    print("\nAll ETL steps completed successfully.")