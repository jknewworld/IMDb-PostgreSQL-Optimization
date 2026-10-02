-- ============================================================================
-- IMDB Query Scenarios
-- Run these queries before and after optimization.
-- Use EXPLAIN ANALYZE to compare performance.
-- ============================================================================

\timing on

-- ============================================================================
-- Scenario 1: Average rating per genre
-- ============================================================================
EXPLAIN ANALYZE
SELECT g.genre,
       COUNT(*) AS title_count,
       ROUND(AVG(tr.average_rating)::numeric, 2) AS avg_rating,
       ROUND(STDDEV(tr.average_rating)::numeric, 2) AS stddev_rating
FROM imdb.title_basics tb
CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
JOIN imdb.title_ratings tr ON tb.tconst = tr.tconst
WHERE tb.title_type = 'movie'
GROUP BY g.genre
ORDER BY avg_rating DESC;

-- ============================================================================
-- Scenario 2: Top 10 movies with most votes (min 1000 votes)
-- ============================================================================
EXPLAIN ANALYZE
SELECT tb.tconst,
       tb.primary_title,
       tb.start_year,
       tr.average_rating,
       tr.num_votes,
       tb.genres
FROM imdb.title_basics tb
JOIN imdb.title_ratings tr ON tb.tconst = tr.tconst
WHERE tb.title_type = 'movie'
  AND tr.num_votes >= 1000
ORDER BY tr.num_votes DESC
LIMIT 10;

-- ============================================================================
-- Scenario 3: Directors with most movies and their average rating
-- ============================================================================
EXPLAIN ANALYZE
SELECT nb.nconst,
       nb.primary_name,
       COUNT(DISTINCT tc.tconst) AS movie_count,
       ROUND(AVG(tr.average_rating)::numeric, 2) AS avg_movie_rating
FROM imdb.name_basics nb
JOIN imdb.title_crew tc ON nb.nconst = ANY(tc.directors)
JOIN imdb.title_ratings tr ON tc.tconst = tr.tconst
JOIN imdb.title_basics tb ON tc.tconst = tb.tconst
WHERE tb.title_type = 'movie'
GROUP BY nb.nconst, nb.primary_name
HAVING COUNT(DISTINCT tc.tconst) >= 5
ORDER BY movie_count DESC
LIMIT 20;

-- ============================================================================
-- Scenario 4: Yearly trend of average movie ratings (2000 onwards)
-- ============================================================================
EXPLAIN ANALYZE
SELECT tb.start_year,
       COUNT(*) AS movie_count,
       ROUND(AVG(tr.average_rating)::numeric, 2) AS avg_rating,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tr.average_rating)::numeric, 2) AS median_rating
FROM imdb.title_basics tb
JOIN imdb.title_ratings tr ON tb.tconst = tr.tconst
WHERE tb.title_type = 'movie'
  AND tb.start_year >= 2000
  AND tr.num_votes >= 100
GROUP BY tb.start_year
ORDER BY tb.start_year;

-- ============================================================================
-- Scenario 5: Actors who have acted in the most genres
-- ============================================================================
EXPLAIN ANALYZE
SELECT nb.nconst,
       nb.primary_name,
       COUNT(DISTINCT g.genre) AS genre_count,
       array_agg(DISTINCT g.genre) AS genres_list
FROM imdb.name_basics nb
JOIN imdb.title_principals tp ON nb.nconst = tp.nconst
JOIN imdb.title_basics tb ON tp.tconst = tb.tconst
CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
WHERE tp.category IN ('actor', 'actress', 'self')
  AND tb.title_type = 'movie'
GROUP BY nb.nconst, nb.primary_name
ORDER BY genre_count DESC
LIMIT 20;

-- ============================================================================
-- Scenario 6: TV series with most seasons
-- ============================================================================
EXPLAIN ANALYZE
SELECT tb.tconst,
       tb.primary_title,
       MAX(te.season_number) AS total_seasons,
       COUNT(DISTINCT te.tconst) AS total_episodes,
       ROUND(AVG(tr.average_rating)::numeric, 2) AS avg_rating
FROM imdb.title_basics tb
JOIN imdb.title_episode te ON tb.tconst = te.parent_tconst
LEFT JOIN imdb.title_ratings tr ON tb.tconst = tr.tconst
WHERE tb.title_type = 'tvseries'
GROUP BY tb.tconst, tb.primary_title
ORDER BY total_seasons DESC
LIMIT 20;

-- ============================================================================
-- Scenario 7: Most popular genre pairs
-- ============================================================================
EXPLAIN ANALYZE
WITH genre_pairs AS (
    SELECT tb.tconst,
           g1.genre AS genre1,
           g2.genre AS genre2
    FROM imdb.title_basics tb
    CROSS JOIN LATERAL unnest(tb.genres) AS g1(genre)
    CROSS JOIN LATERAL unnest(tb.genres) AS g2(genre)
    WHERE tb.title_type = 'movie'
      AND g1.genre < g2.genre
)
SELECT genre1,
       genre2,
       COUNT(*) AS pair_count,
       ROUND(AVG(tr.average_rating)::numeric, 2) AS avg_rating
FROM genre_pairs gp
JOIN imdb.title_ratings tr ON gp.tconst = tr.tconst
GROUP BY genre1, genre2
ORDER BY pair_count DESC
LIMIT 20;

-- ============================================================================
-- Scenario 8: Longest movies by genre
-- ============================================================================
EXPLAIN ANALYZE
WITH ranked_movies AS (
    SELECT g.genre,
           tb.tconst,
           tb.primary_title,
           tb.runtime_minutes,
           tr.average_rating,
           ROW_NUMBER() OVER (
               PARTITION BY g.genre
               ORDER BY tb.runtime_minutes DESC
           ) AS rn
    FROM imdb.title_basics tb
    CROSS JOIN LATERAL unnest(tb.genres) AS g(genre)
    LEFT JOIN imdb.title_ratings tr ON tb.tconst = tr.tconst
    WHERE tb.title_type = 'movie'
      AND tb.runtime_minutes IS NOT NULL
)
SELECT genre,
       tconst,
       primary_title,
       runtime_minutes,
       average_rating
FROM ranked_movies
WHERE rn <= 5
ORDER BY genre, runtime_minutes DESC;

-- ============================================================================
-- Helper: Check queue status
-- ============================================================================
SELECT * FROM imdb.queue_metrics();
