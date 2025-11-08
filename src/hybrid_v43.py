from dataclasses import dataclass
from typing import Dict, Any
from .t_model import t_model_total_mc, market_fusion, pick_total
from .s_model import ats_mc, pick_spread

@dataclass
class TModelConfig:
    n: int = 200_000
    df_t: int = 5
    sd_total: float = 20.1
    w_market: float = 0.65

@dataclass
class SModelConfig:
    n: int = 200_000
    df_t: int = 5
    sd_team: float = 12.0
    rho: float = 0.40

class HybridV43:
    def __init__(self, t_cfg: TModelConfig, s_cfg: SModelConfig):
        self.t_cfg = t_cfg
        self.s_cfg = s_cfg

    def run_totals(self, mu_total: float, market_total: float) -> Dict[str, Any]:
        fused = market_fusion(mu_total, market_total, self.t_cfg.w_market)
        samples = t_model_total_mc(fused, sd_total=self.t_cfg.sd_total, df_t=self.t_cfg.df_t, n=self.t_cfg.n)
        pick, prob, ev = pick_total(samples, market_total)
        return {"T_model_mu": fused, "T_pick": pick, "T_prob": prob, "T_EV": ev}

    def run_spread(self, mu_margin: float, market_spread: float, fav_is_home: bool) -> Dict[str, Any]:
        samples = ats_mc(mu_margin, sd_team=self.s_cfg.sd_team, df_t=self.s_cfg.df_t, n=self.s_cfg.n)
        pick, prob, ev = pick_spread(samples, market_spread, fav_is_home=fav_is_home)
        return {"S_margin_mu": mu_margin, "S_pick": pick, "S_prob": prob, "S_EV": ev}
