-- Полная очистка с resetом последовательностей и обходом внешних ключей
BEGIN;

	TRUNCATE TABLE app.areas, 
	               games, 
	               teams, 
	               tournament_teams, 
	               tournaments
	RESTART IDENTITY 
	CASCADE;

COMMIT;