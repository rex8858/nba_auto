import numpy as np

def ats_mc(mu_margin: float, sd_team: float=12.0, rho: float=0.40, df_t: int=5, n: int=200_000):
    return mu_margin + (np.random.standard_t(df_t, size=n) * sd_team)

def pick_spread(samples, market_spread: float, fav_is_home: bool=True, price: int=-110):
    cover_prob = (samples > market_spread).mean() if fav_is_home else ((-samples) > (-market_spread)).mean()
    r = price/100.0 if price>0 else 100.0/abs(price)
    ev_val = cover_prob*r - (1-cover_prob)*1.0
    pick_str = f"{'HOME' if fav_is_home else 'AWAY'} {market_spread:+.1f}"
    return pick_str, float(cover_prob), float(ev_val)
