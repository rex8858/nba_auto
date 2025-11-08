from .sources import nba_dot_com, espn, rotowire, vegasinsider

def get_today_schedule_et():
    return []

def get_closing_lines(date_et: str):
    return vegasinsider.fetch_closing_lines(date_et)

def get_injuries_lineups(date_et: str):
    return rotowire.fetch_injuries_and_lineups(date_et)

def get_scores_odds(date_et: str):
    return espn.fetch_scores_and_odds(date_et)
