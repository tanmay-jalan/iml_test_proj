import os
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

SCORE_DIR = "data/scores"

box_scores = os.listdir(SCORE_DIR)
box_scores = [os.path.join(SCORE_DIR, f) for f in box_scores if f.endswith(".html")]

def parse_html(box_score):
    with open(box_score, encoding="utf-8") as f:
        html = f.read()
    soup = BeautifulSoup(html, features="lxml")
    [s.decompose() for s in soup.select("tr.over_header")]
    [s.decompose() for s in soup.select("tr.thead")]
    return soup

def read_line_score_table(soup):
    line_score = pd.read_html(StringIO(str(soup)), attrs={"id": "line_score"})[0]
    columns = list(line_score.columns)
    columns[0] = "team"
    columns[-1] = "total"
    line_score.columns = columns
    line_score = line_score[["team", "total"]]
    return line_score

def read_stats(soup, team, stat):
    table_id = f"box-{team}-game{stat}"
    table = soup.find("table", {"id": table_id})
    if table is None:
        raise ValueError(f"Table with id '{table_id}' not found in the HTML.")
    df = pd.read_html(StringIO(str(table)), index_col=0)[0]
    df = df.apply(pd.to_numeric, errors="coerce")
    return df


def read_szn(soup):
    nav = soup.select("#bottom_nav_container")[0]
    hrefs = [a["href"] for a in nav.find_all("a")]
    season = os.path.basename(hrefs[1]).split("_")[0]
    return season

base_cols = None
games = []

for box_score in box_scores:
    soup = parse_html(box_score)
    line_score = read_line_score_table(soup)
    teams = list(line_score["team"])
    summaries = []

    for team in teams:
        basic = read_stats(soup, team, "-basic")
        advanced = read_stats(soup, team, "-advanced")

        totals = pd.concat([basic.iloc[-1, :], advanced.iloc[-1, :]])
        totals.index = totals.index.str.lower()

        maxes = pd.concat([basic.iloc[:-1, :].max(), advanced.iloc[:-1, :].max()])
        maxes.index = maxes.index.str.lower() + "_max"

        summary = pd.concat([totals, maxes])

        if base_cols is None:
            base_cols = list(summary.index.drop_duplicates(keep="first"))
            base_cols = [b for b in base_cols if "bpm" not in b]

        summary = summary[base_cols]
        summaries.append(summary)

    summary = pd.concat(summaries, axis=1).T
    game = pd.concat([summary, line_score], axis=1)
    game["home"] = [0, 1]
    game_opp = game.iloc[::-1].reset_index()
    game_opp.columns += "_opp"

    full_game = pd.concat([game, game_opp], axis=1)
    full_game["season"] = read_szn(soup)
    full_game["date"] = os.path.basename(box_score[:8])
    full_game["date"] = pd.to_datetime(full_game["date"], format="%Y%m%d", errors = "coerce")
    full_game["won"] = full_game["total"] > full_game["total_opp"]

    games.append(full_game)

    if len(games) % 100 == 0:
        print(f"{len(games)} / {len(box_scores)}")

games_df = pd.concat(games, ignore_index=True)
games_df.to_csv("nba_games.csv")
