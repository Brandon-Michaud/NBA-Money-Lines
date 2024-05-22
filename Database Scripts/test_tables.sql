INSERT INTO Teams
VALUES ('Oklahoma City Thunder')

INSERT INTO Games
VALUES ('2024-05-18', 'Dallas Mavericks', 'Oklahoma City Thunder', 10, 9)

SELECT *
FROM Teams;

SELECT *
FROM Players;

SELECT count(*)
FROM Games;

SELECT *
FROM PlayerStats;

SELECT *
FROM TeamStats;



DELETE
FROM PlayerStats;

DELETE
FROM TeamStats;

DELETE
FROM Games;

DELETE
FROM Players;

DELETE
FROM Teams;