import numpy as np

def t_model_total_mc(mu_total: float, sd_total: float = 20.1, df_t: int = 5, n: int = 200_000):
    return mu_total + (np.random.standard_t(df_t, size=n) * sd_total)

def market_fusion(mu_model: float, market_total: float, w_market: float = 0.65) -> float:
    return w_market*market_total + (1-w_market)*mu_model

def pick_total(samples, market_total: float, price_over: int=-110, price_under: int=-110):
    p_over = (samples > market_total).mean()
    p_under = 1 - p_over
    def ev(p, american):
        r = american/100.0 if american>0 else 100.0/abs(american)
        return p*r - (1-p)*1.0
    ev_over = ev(p_over, price_over)
    ev_under = ev(p_under, price_under)
    return ("OVER", float(p_over), float(ev_over)) if ev_over>=ev_under else ("UNDER", float(p_under), float(ev_under))
