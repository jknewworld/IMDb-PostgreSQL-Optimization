-- ============================================================================
-- IMDb PostgreSQL Project - Eight Required Query Scenarios
-- This file returns the actual results of all eight required scenarios.
-- Use sql/scenarios.sql for EXPLAIN (ANALYZE, BUFFERS).
-- ============================================================================

\timing on
SET search_path TO imdb, public;

-- ----------------------------------------------------------------------------
-- Scenario 1: Average movie rating grouped by genre
-- Output: genre, rated_movie_count, average_rating
-- ----------------------------------------------------------------------------
SELECT g.genre,
       COUNT(*) AS rated_movie_count,
       ROUND(AVG(tr.average_rating), 2) AS average_rating
FROM imdb.title_basics AS tb
CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
JOIN imdb.title_ratings AS tr ON tr.tconst = tb.tconst
WHERE tb.title_type = 'movie'
  AND g.genre IS NOT NULL
  AND tr.average_rating IS NOT NULL
GROUP BY g.genre
ORDER BY average_rating DESC, g.genre;

-- ----------------------------------------------------------------------------
-- Scenario 2: Ten movies with the greatest vote count, minimum 1000 votes
-- Output: tconst, title, release year, rating, votes, genres
-- ----------------------------------------------------------------------------
SELECT tb.tconst,
       tb.primary_title,
       tb.start_year,
       tr.average_rating,
       tr.num_votes,
       tb.genres
FROM imdb.title_ratings AS tr
JOIN imdb.title_basics AS tb ON tb.tconst = tr.tconst
WHERE tr.num_votes >= 1000
  AND tb.title_type = 'movie'
ORDER BY tr.num_votes DESC, tb.tconst
LIMIT 10;

-- ----------------------------------------------------------------------------
-- Scenario 3: Twenty directors with the greatest number of movies
-- Output: nconst, name, movie count, average rating of rated works
-- ----------------------------------------------------------------------------
WITH movie_directors AS (
    SELECT DISTINCT tc.tconst, d.nconst
    FROM imdb.title_crew AS tc
    CROSS JOIN LATERAL unnest(tc.directors) AS d(nconst)
    JOIN imdb.title_basics AS tb ON tb.tconst = tc.tconst
    WHERE tb.title_type = 'movie'
      AND d.nconst IS NOT NULL
)
SELECT nb.nconst,
       nb.primary_name,
       COUNT(*) AS movie_count,
       ROUND(AVG(tr.average_rating), 2) AS average_movie_rating
FROM movie_directors AS md
JOIN imdb.name_basics AS nb ON nb.nconst = md.nconst
LEFT JOIN imdb.title_ratings AS tr ON tr.tconst = md.tconst
GROUP BY nb.nconst, nb.primary_name
ORDER BY movie_count DESC, nb.nconst
LIMIT 20;

-- ----------------------------------------------------------------------------
-- Scenario 4: Annual average movie rating from 2000 onward
-- Output: year, rated movie count, average rating
-- ----------------------------------------------------------------------------
SELECT tb.start_year,
       COUNT(*) AS rated_movie_count,
       ROUND(AVG(tr.average_rating), 2) AS average_rating
FROM imdb.title_basics AS tb
JOIN imdb.title_ratings AS tr ON tr.tconst = tb.tconst
WHERE tb.title_type = 'movie'
  AND tb.start_year >= 2000
  AND tr.average_rating IS NOT NULL
GROUP BY tb.start_year
ORDER BY tb.start_year;

-- ----------------------------------------------------------------------------
-- Scenario 5: Actors who have acted in more than five distinct movie genres
-- Assumption: categories actor and actress are counted; self is excluded.
-- Output: nconst, name, distinct genre count, sorted genre list
-- ----------------------------------------------------------------------------
SELECT nb.nconst,
       nb.primary_name,
       COUNT(DISTINCT g.genre) AS distinct_genre_count,
       ARRAY_AGG(DISTINCT g.genre ORDER BY g.genre) AS genres
FROM imdb.title_principals AS tp
JOIN imdb.name_basics AS nb ON nb.nconst = tp.nconst
JOIN imdb.title_basics AS tb ON tb.tconst = tp.tconst
CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
WHERE tp.category IN ('actor', 'actress')
  AND tb.title_type = 'movie'
  AND g.genre IS NOT NULL
GROUP BY nb.nconst, nb.primary_name
HAVING COUNT(DISTINCT g.genre) > 5
ORDER BY distinct_genre_count DESC, nb.nconst;

-- ----------------------------------------------------------------------------
-- Scenario 6: Series with the greatest number of distinct seasons
-- Output: series id, title, distinct season count, episode count, rating
-- ----------------------------------------------------------------------------
SELECT tb.tconst,
       tb.primary_title,
       COUNT(DISTINCT te.season_number)
           FILTER (WHERE te.season_number IS NOT NULL AND te.season_number > 0)
           AS season_count,
       COUNT(*) AS episode_count,
       tr.average_rating
FROM imdb.title_episode AS te
JOIN imdb.title_basics AS tb ON tb.tconst = te.parent_tconst
LEFT JOIN imdb.title_ratings AS tr ON tr.tconst = tb.tconst
WHERE tb.title_type IN ('tvSeries', 'tvMiniSeries', 'tvseries', 'tvminiseries')
GROUP BY tb.tconst, tb.primary_title, tr.average_rating
ORDER BY season_count DESC, episode_count DESC, tb.tconst
LIMIT 20;

-- ----------------------------------------------------------------------------
-- Scenario 7: Most common unordered pairs of movie genres
-- Each genre is de-duplicated inside a title and each pair is generated once.
-- Output: genre_1, genre_2, number of movies
-- ----------------------------------------------------------------------------
WITH movie_genres AS (
    SELECT tb.tconst,
           ARRAY(
               SELECT DISTINCT genre
               FROM unnest(tb.genres) AS genre
               WHERE genre IS NOT NULL
               ORDER BY genre
           ) AS genres
    FROM imdb.title_basics AS tb
    WHERE tb.title_type = 'movie'
      AND cardinality(tb.genres) >= 2
), genre_pairs AS (
    SELECT mg.tconst,
           g1.genre AS genre_1,
           g2.genre AS genre_2
    FROM movie_genres AS mg
    CROSS JOIN LATERAL unnest(mg.genres) WITH ORDINALITY AS g1(genre, position)
    CROSS JOIN LATERAL unnest(mg.genres) WITH ORDINALITY AS g2(genre, position)
    WHERE g1.position < g2.position
)
SELECT genre_1,
       genre_2,
       COUNT(*) AS movie_count
FROM genre_pairs
GROUP BY genre_1, genre_2
ORDER BY movie_count DESC, genre_1, genre_2
LIMIT 20;

-- ----------------------------------------------------------------------------
-- Scenario 8: Five longest movies in each genre
-- ROW_NUMBER returns exactly five rows per genre when at least five exist.
-- Output: genre, rank, id, title, runtime, release year, rating
-- ----------------------------------------------------------------------------
WITH ranked_movies AS (
    SELECT g.genre,
           tb.tconst,
           tb.primary_title,
           tb.runtime_minutes,
           tb.start_year,
           tr.average_rating,
           ROW_NUMBER() OVER (
               PARTITION BY g.genre
               ORDER BY tb.runtime_minutes DESC, tb.tconst
           ) AS genre_rank
    FROM imdb.title_basics AS tb
    CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
    LEFT JOIN imdb.title_ratings AS tr ON tr.tconst = tb.tconst
    WHERE tb.title_type = 'movie'
      AND tb.runtime_minutes IS NOT NULL
      AND g.genre IS NOT NULL
)
SELECT genre,
       genre_rank,
       tconst,
       primary_title,
       runtime_minutes,
       start_year,
       average_rating
FROM ranked_movies
WHERE genre_rank <= 5
ORDER BY genre, genre_rank;

-- Queue operational summary
SELECT * FROM imdb.queue_metrics();
