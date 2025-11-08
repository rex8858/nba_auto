import argparse, os, pandas as pd, yaml, pytz
from datetime import datetime
from src.hybrid_v43 import HybridV43, TModelConfig, SModelConfig
from src.utils import write_csv, sha256_file

def load_settings(path='config/settings.yaml'):
    with open(path,'r') as f:
        return yaml.safe_load(f)

def ensure_files(master_path, per_game_path):
    for p in [master_path, per_game_path]:
        if not os.path.exists(p):
            pd.DataFrame().to_csv(p, index=False)

def run_morning(settings):
    now = datetime.now(pytz.timezone(settings['timezone'])).strftime('%Y-%m-%d %H:%M:%S')
    master = pd.read_csv(settings['files']['master'])
    row = dict(snapshot_type='T60', snapshot_time=now, note='placeholder run')
    master = pd.concat([master, pd.DataFrame([row])], ignore_index=True)
    write_csv(master, settings['files']['master'])
    return ['morning_done']

def run_evening(settings):
    now = datetime.now(pytz.timezone(settings['timezone'])).strftime('%Y-%m-%d %H:%M:%S')
    master = pd.read_csv(settings['files']['master'])
    master = master[master.get('summary_type','') != 'TOTALS']
    row = dict(summary_type='TOTALS', snapshot_time=now, note='placeholder totals')
    master = pd.concat([master, pd.DataFrame([row])], ignore_index=True)
    write_csv(master, settings['files']['master'])
    return ['evening_done']

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--task', choices=['morning','evening'], required=True)
    args = ap.parse_args()
    settings = load_settings()
    ensure_files(settings['files']['master'], settings['files']['per_game'])
    if args.task=='morning':
        run_morning(settings)
    else:
        run_evening(settings)
    print('SHA256(master)=', sha256_file(settings['files']['master']))

if __name__ == '__main__':
    main()
