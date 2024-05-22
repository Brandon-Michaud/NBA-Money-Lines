-- Step 1: Create a temporary table to store the game references for Marcus Williams
CREATE TEMPORARY TABLE GameReferences AS
SELECT game_date, home_team_name, away_team_name
FROM PlayerStats
WHERE player_name = 'Marcus Williams';

-- Step 2: Delete team stats for the games that Marcus Williams played in
DELETE FROM TeamStats
WHERE (game_date, home_team_name, away_team_name) IN (
    SELECT game_date, home_team_name, away_team_name
    FROM GameReferences
);

-- Step 3: Delete player stats for Marcus Williams
DELETE FROM PlayerStats
WHERE (game_date, home_team_name, away_team_name) IN (
    SELECT game_date, home_team_name, away_team_name
    FROM GameReferences
);

-- Step 4: Delete games that Marcus Williams played in
DELETE FROM Games
WHERE (game_date, home_team_name, away_team_name) IN (
    SELECT game_date, home_team_name, away_team_name
    FROM GameReferences
);

-- Drop the temporary table after use
DROP TABLE GameReferences;
