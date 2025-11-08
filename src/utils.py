import pandas as pd
from hashlib import sha256

def american_ev(prob, american_odds=-110):
    if american_odds > 0: r = american_odds/100.0
    else: r = 100.0/abs(american_odds)
    return prob*r - (1-prob)*1.0

def write_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

def sha256_file(path: str) -> str:
    h = sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()
