DROP TABLE IF EXISTS PlayerStats;
DROP TABLE IF EXISTS TeamStats;
DROP TABLE IF EXISTS Games;
DROP TABLE IF EXISTS Teams;
DROP TABLE IF EXISTS Players;

CREATE TABLE Teams (
    team_name VARCHAR(255) PRIMARY KEY
);

CREATE TABLE Players (
    player_name VARCHAR(255) PRIMARY KEY
);

CREATE TABLE Games (
    game_date DATE,
    home_team_name VARCHAR(255) REFERENCES Teams(team_name),
    away_team_name VARCHAR(255) REFERENCES Teams(team_name),
    home_team_score INT,
    away_team_score INT,
    PRIMARY KEY (game_date, home_team_name, away_team_name)
);

CREATE TABLE TeamStats (
    game_date DATE,
    team_name VARCHAR(255),
    home_team_name VARCHAR(255),
	away_team_name VARCHAR(255),
	home BOOLEAN,
	field_goals_made INT,
	field_goals_attempted INT,
	field_goal_percentage FLOAT,
	three_pointers_made INT,
	three_pointers_attempted INT,
	three_pointer_percentage FLOAT,
	free_throws_made INT,
	free_throws_attempted INT,
	free_throw_percentage FLOAT,
	offensive_rebounds INT,
	defensive_rebounds INT,
	total_rebounds INT,
	assists INT,
	steals INT,
	blocks INT,
	turnovers INT,
	personal_fouls INT,
	points INT,
	true_shooting_percentage FLOAT,
	effective_field_goal_percentage FLOAT,
	three_point_attempt_rate FLOAT,
	free_throw_rate FLOAT,
	offensive_rebound_percentage FLOAT,
	defensive_rebound_percentage FLOAT,
	total_rebound_percentage FLOAT,
	assist_percentage FLOAT,
	steal_percentage FLOAT,
	block_percentage FLOAT,
	turnover_percentage FLOAT,
	offensive_rating FLOAT,
	defensive_rating FLOAT,
    PRIMARY KEY (game_date, team_name),
    FOREIGN KEY (game_date, home_team_name, away_team_name) REFERENCES Games(game_date, home_team_name, away_team_name),
    FOREIGN KEY (team_name) REFERENCES Teams(team_name)
);

CREATE TABLE PlayerStats (
    game_date DATE,
    player_name VARCHAR(255),
    home_team_name VARCHAR(255),
	away_team_name VARCHAR(255),
	home BOOLEAN,
	minutes_played FLOAT,
	field_goals_made INT,
	field_goals_attempted INT,
	field_goal_percentage FLOAT,
	three_pointers_made INT,
	three_pointers_attempted INT,
	three_pointer_percentage FLOAT,
	free_throws_made INT,
	free_throws_attempted INT,
	free_throw_percentage FLOAT,
	offensive_rebounds INT,
	defensive_rebounds INT,
	total_rebounds INT,
	assists INT,
	steals INT,
	blocks INT,
	turnovers INT,
	personal_fouls INT,
	points INT,
	plus_minus INT,
	true_shooting_percentage FLOAT,
	effective_field_goal_percentage FLOAT,
	three_point_attempt_rate FLOAT,
	free_throw_rate FLOAT,
	offensive_rebound_percentage FLOAT,
	defensive_rebound_percentage FLOAT,
	total_rebound_percentage FLOAT,
	assist_percentage FLOAT,
	steal_percentage FLOAT,
	block_percentage FLOAT,
	turnover_percentage FLOAT,
	usage_percentage FLOAT,
	offensive_rating FLOAT,
	defensive_rating FLOAT,
	box_plus_minus FLOAT,
    PRIMARY KEY (game_date, player_name),
    FOREIGN KEY (game_date, home_team_name, away_team_name) REFERENCES Games(game_date, home_team_name, away_team_name),
    FOREIGN KEY (player_name) REFERENCES Players(player_name)
);
