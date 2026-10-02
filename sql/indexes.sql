-- IMDb PostgreSQL Project  Index Design
SET search_path TO imdb, public;

-- Queue support: B Tree partial indexes.
CREATE INDEX IF NOT EXISTS idx_message_queue_pending_ready
    ON imdb.message_queue(queue_name, available_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_message_queue_processing_timeout
    ON imdb.message_queue(queue_name, processing_started_at, id)
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS idx_message_queue_status_metrics
    ON imdb.message_queue(queue_name, status);

-- Scenarios 1, 3, 4, 7 and 8: restrict very large title table to movies
CREATE INDEX IF NOT EXISTS idx_title_basics_movie_year
    ON imdb.title_basics(start_year, tconst)
    INCLUDE (primary_title, genres, runtime_minutes)
    WHERE title_type = 'movie';

-- Scenario 8: assists access ordered by runtime for movie rows
CREATE INDEX IF NOT EXISTS idx_title_basics_movie_runtime
    ON imdb.title_basics(runtime_minutes DESC, tconst)
    INCLUDE (primary_title, genres)
    WHERE title_type = 'movie' AND runtime_minutes IS NOT NULL;

-- Scenario 2: top N query with num_votes >= 1000
CREATE INDEX IF NOT EXISTS idx_title_ratings_top_votes
    ON imdb.title_ratings(num_votes DESC, tconst)
    INCLUDE (average_rating)
    WHERE num_votes >= 1000;

-- Scenario 5: actor/actress principals only
CREATE INDEX IF NOT EXISTS idx_title_principals_actor_lookup
    ON imdb.title_principals(nconst, tconst)
    WHERE category IN ('actor', 'actress');

-- Scenario 6: grouping episodes by parent and counting distinct seasons
CREATE INDEX IF NOT EXISTS idx_title_episode_parent_season
    ON imdb.title_episode(parent_tconst, season_number)
    INCLUDE (tconst, episode_number);

CREATE INDEX IF NOT EXISTS idx_title_basics_genres_gin
    ON imdb.title_basics USING GIN (genres);

CREATE INDEX IF NOT EXISTS idx_title_crew_directors_gin
    ON imdb.title_crew USING GIN (directors);


CREATE INDEX IF NOT EXISTS idx_title_basics_metadata_gin
    ON imdb.title_basics USING GIN (metadata jsonb_path_ops);

-- Operational log lookup
CREATE INDEX IF NOT EXISTS idx_event_log_type_created
    ON imdb.event_log(event_type, created_at DESC);

-- Refresh planner statistics
ANALYZE imdb.title_basics;
ANALYZE imdb.title_ratings;
ANALYZE imdb.name_basics;
ANALYZE imdb.title_crew;
ANALYZE imdb.title_principals;
ANALYZE imdb.title_akas;
ANALYZE imdb.title_episode;
ANALYZE imdb.message_queue;

SELECT 'Analytical indexes created and statistics refreshed' AS status;
