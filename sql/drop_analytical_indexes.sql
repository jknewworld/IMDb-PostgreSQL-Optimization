SET search_path TO imdb, public;

DROP INDEX IF EXISTS imdb.idx_title_basics_movie_year;
DROP INDEX IF EXISTS imdb.idx_title_basics_movie_runtime;
DROP INDEX IF EXISTS imdb.idx_title_ratings_top_votes;
DROP INDEX IF EXISTS imdb.idx_title_principals_actor_lookup;
DROP INDEX IF EXISTS imdb.idx_title_episode_parent_season;
DROP INDEX IF EXISTS imdb.idx_title_basics_genres_gin;
DROP INDEX IF EXISTS imdb.idx_title_crew_directors_gin;
DROP INDEX IF EXISTS imdb.idx_title_basics_metadata_gin;
DROP INDEX IF EXISTS imdb.idx_event_log_type_created;

ANALYZE imdb.title_basics;
ANALYZE imdb.title_ratings;
ANALYZE imdb.name_basics;
ANALYZE imdb.title_crew;
ANALYZE imdb.title_principals;
ANALYZE imdb.title_episode;

SELECT 'Analytical indexes dropped; baseline state restored' AS status;
