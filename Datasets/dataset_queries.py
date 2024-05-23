all_games = """
SELECT game_date, home_team_name, away_team_name, home_team_score, away_team_score
FROM Games;
"""

team_points_per_game = """
SELECT AVG(points)
FROM (
    SELECT points
    FROM TeamStats
    WHERE team_name = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
) as last_x_points;
"""

team_points_per_game_home_away = """
SELECT AVG(points)
FROM (
    SELECT points
    FROM TeamStats
    WHERE team_name = %s
    AND home = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
) as last_x_points_home_away;
"""

team_points_conceded_per_game = """
WITH LastXGames AS (
    SELECT
           CASE
               WHEN home_team_name = %s THEN away_team_score
               ELSE home_team_score
           END AS points_conceded
    FROM Games
    WHERE (home_team_name = %s OR away_team_name = %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
)
SELECT AVG(points_conceded)
FROM LastXGames;
"""

team_points_conceded_per_game_home = """
SELECT AVG(away_team_score)
FROM (
    SELECT away_team_score
    FROM Games
    WHERE home_team_name = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
) as last_x_points_conceded_home;
"""

team_points_conceded_per_game_away = """
SELECT AVG(home_team_score)
FROM (
    SELECT home_team_score
    FROM Games
    WHERE away_team_name = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
) as last_x_points_conceded_away;
"""

team_win_percentage = """
WITH LastXGames AS (
    SELECT
           CASE
               WHEN home_team_name = %s AND home_team_score > away_team_score THEN 1
               WHEN away_team_name = %s AND away_team_score > home_team_score THEN 1
               ELSE 0
           END AS win
    FROM Games
    WHERE (home_team_name = %s OR away_team_name = %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
)
SELECT SUM(win) * 1.0 / COUNT(*) AS winning_percentage
FROM LastXGames;
"""

team_win_percentage_home = """
WITH LastXGames AS (
    SELECT
           CASE
               WHEN home_team_score > away_team_score THEN 1
               ELSE 0
           END AS win
    FROM Games
    WHERE home_team_name = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
)
SELECT SUM(win) * 1.0 / COUNT(*) AS winning_percentage
FROM LastXGames;
"""

team_win_percentage_away = """
WITH LastXGames AS (
    SELECT
           CASE
               WHEN away_team_score > home_team_score THEN 1
               ELSE 0
           END AS win
    FROM Games
    WHERE away_team_name = %s
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT %s
)
SELECT SUM(win) * 1.0 / COUNT(*) AS winning_percentage
FROM LastXGames;
"""

team_days_since_last_game = """
WITH LastGame AS (
    SELECT game_date
    FROM Games
    WHERE (home_team_name = %s OR away_team_name = %s)
    AND game_date < %s
    ORDER BY game_date DESC
    LIMIT 1
)
SELECT %s - game_date
FROM LastGame;
"""