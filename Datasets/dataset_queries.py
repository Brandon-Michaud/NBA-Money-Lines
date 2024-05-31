all_games = """
SELECT game_date, home_team_name, away_team_name, home_team_score, away_team_score
FROM Games;
"""

# .format(home_stat_columns, away_stat_columns)
all_games_with_stats = """
SELECT 
    Games.game_date, 
    Games.home_team_name, 
    Games.away_team_name, 
    {home_stat_columns},
    {away_stat_columns}
FROM 
    Games
JOIN 
    TeamStats home_stats ON Games.game_date = home_stats.game_date AND Games.home_team_name = home_stats.team_name
JOIN 
    TeamStats away_stats ON Games.game_date = away_stats.game_date AND Games.away_team_name = away_stats.team_name;
"""

# .format(stat_columns, aggregate_stat_columns)
# parameters = (team_name, include_home_games, team_name, include_away_games, game_date, window, team_name)
team_stat_per_game = """
WITH LastXGames AS (
    SELECT game_date
    FROM Games
    WHERE (home_team_name = %s AND %s OR away_team_name = %s AND %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
),
TeamStatsLastXGames AS (
    SELECT {stat_columns}
    FROM TeamStats
    JOIN LastXGames ON TeamStats.game_date = LastXGames.game_date
    WHERE TeamStats.team_name = %s
)
SELECT {aggregate_stat_columns}
FROM TeamStatsLastXGames;
"""

# .format(stat_columns, aggregate_stat_columns)
# parameters = (team_name, game_date, include_home_games, include_away_games, window)
team_stat_per_game_alt = """
SELECT {aggregate_stat_columns}
FROM (
    SELECT {stat_columns}
    FROM TeamStats
    WHERE team_name = %s
    AND game_date < %s
    AND ((home = TRUE AND %s) or (home = FALSE AND %s))
    ORDER BY game_date DESC
    LIMIT %s
) as LastXGames
"""

# .format(stat_columns, aggregate_stat_columns)
# parameters = (team_name, team_name, include_home_games, team_name, include_away_games, game_date, window)
team_stat_conceded_per_game = """
WITH LastXGames AS (
    SELECT game_date,
           CASE
               WHEN home_team_name = %s THEN away_team_name
               ELSE home_team_name
           END AS opponent
    FROM Games
    WHERE (home_team_name = %s AND %s OR away_team_name = %s AND %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
),
StatConceded AS (
    SELECT {stat_columns}
    FROM LastXGames
    JOIN TeamStats ON LastXGames.game_date = TeamStats.game_date AND LastXGames.opponent = TeamStats.team_name
)
SELECT {aggregate_stat_columns}
FROM StatConceded;
"""

# (team_name, team_name, team_name, include_home_games, team_name, include_away_games, game_date, window)
team_win_percentage = """
WITH LastXGames AS (
    SELECT
           CASE
               WHEN home_team_name = %s AND home_team_score > away_team_score THEN 1
               WHEN away_team_name = %s AND away_team_score > home_team_score THEN 1
               ELSE 0
           END AS win
    FROM Games
    WHERE (home_team_name = %s AND %s OR away_team_name = %s AND %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
)
SELECT SUM(win) * 1.0 / COUNT(*)
FROM LastXGames;
"""

# (team_name, team_name, game_date, game_date)
team_days_since_last_game = """
WITH LastGame AS (
    SELECT game_date
    FROM Games
    WHERE (home_team_name = %s OR away_team_name = %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT 1
)
SELECT %s - game_date AS days_since_last_game
FROM LastGame;
"""

# .format(stat_columns, aggregate_stat_columns)
# parameters = (game_date, include_home_games, include_away_games, game_date, average_window)
every_team_every_thing = """
WITH RankedGames AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY G.team_name ORDER BY G.game_date DESC) AS game_rank,
        G.team_name,
        G.game_date,
        {stat_columns}
    FROM (
        SELECT 
            home_team_name AS team_name,
            game_date,
            (CASE WHEN home_team_score > away_team_score THEN 1 ELSE 0 END) AS win,
            away_team_name AS opponent_team
        FROM 
            Games
        WHERE 
            game_date < %s
		AND %s
        UNION ALL
        SELECT 
            away_team_name AS team_name,
            game_date,
            (CASE WHEN away_team_score > home_team_score THEN 1 ELSE 0 END) AS win,
            home_team_name AS opponent_team
        FROM 
            Games
        WHERE 
            game_date < %s
		AND %s
    ) AS G
    JOIN TeamStats T ON G.game_date = T.game_date AND G.team_name = T.team_name
    JOIN TeamStats O ON G.game_date = O.game_date AND G.opponent_team = O.team_name
),
LastXGames AS (
    SELECT 
        team_name,
        {aggregate_stat_columns}
    FROM 
        RankedGames
    WHERE 
        game_rank <= %s
    GROUP BY 
        team_name
)
SELECT *
FROM 
    LastXGames;
"""

# .format(stat_columns, aggregate_stat_columns)
# parameters = (game_date, home_team_name, away_team_name, players_for_home_or_away,
#               game_date, include_home_games, include_away_games, window)
players_stats_per_game = """
WITH PlayersInGame AS (
    SELECT player_name
    FROM PlayerStats
    WHERE game_date = %s
    AND home_team_name = %s
    AND away_team_name = %s
	AND home = %s
),
PlayerLastXGames AS (
    SELECT 
        ps.player_name,
        {stat_columns},
        ROW_NUMBER() OVER (PARTITION BY ps.player_name ORDER BY ps.game_date DESC) AS game_rank
    FROM PlayerStats ps
    JOIN PlayersInGame pig ON ps.player_name = pig.player_name
    WHERE ps.game_date < %s
    AND ((home = TRUE AND %s) or (home = FALSE AND %s))
)
SELECT 
    player_name,
    {aggregate_stat_columns}
FROM 
    PlayerLastXGames
WHERE 
    game_rank <= %s
GROUP BY 
    player_name;
"""