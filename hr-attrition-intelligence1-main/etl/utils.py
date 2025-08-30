from pathlib import Path
from datetime import datetime
import os
import pandas as pd

def save_with_backup(df: pd.DataFrame, latest_path: Path, backup_dir: Path, prefix: str = None):
    """
    Save DataFrame to 'latest_path' and also save a timestamped backup in 'backup_dir'.
    If 'prefix' is given, the backup file will be named '{prefix}_{timestamp}.csv'.
    """
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if prefix:
        backup_file = backup_dir / f"{prefix}_{timestamp}.csv"
    else:
        backup_file = backup_dir / f"backup_{timestamp}.csv"
    # Always backup BEFORE overwriting latest
    df.to_csv(backup_file, index=False)
    df.to_csv(latest_path, index=False)
    print(f"Saved latest to {latest_path} and backup to {backup_file}")
