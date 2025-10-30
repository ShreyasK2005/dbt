/* ============================================================================
   FAST SAMPLE RUN: Parse a few PDFs → Enrich for CTO → Build search service
   - Only processes a small subset so it runs quickly
   - Objects suffixed with _SAMPLE to avoid collisions
   Run top → bottom in one worksheet tab.
   ============================================================================ */

-- Workspace context (adjust if yours differs)
USE ROLE TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH_XS;
USE DATABASE CORTEX_SEARCH_DEMO;
USE SCHEMA PEOPLE;

/* ----------------------
   Configure sample size
   ---------------------- */
SET SAMPLE_SIZE = 5;         -- change to 2/3/10 etc.

/* ============================================================================
   1) Pick a small subset of PDFs from the stage
   - QUALIFY ROW_NUMBER() chooses the first N by name (stable + simple)
   - You can also filter by file name pattern in the WHERE if you want
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE SAMPLE_FILES AS
WITH files AS (
  SELECT relative_path
  FROM directory('@CORTEX_SEARCH_DEMO.PEOPLE.INTERNAL_PEOPLE_STAGE')
  WHERE relative_path ILIKE '%.pdf'          -- narrow further if desired (e.g., '%john_doe%.pdf')
)
SELECT relative_path
FROM files
QUALIFY ROW_NUMBER() OVER (ORDER BY relative_path) <= $SAMPLE_SIZE;

-- Quick peek
SELECT * FROM SAMPLE_FILES;

/* ============================================================================
   2) Parse only the sample PDFs → text (FAST)
   ============================================================================ */
CREATE OR REPLACE TABLE PARSED_RESUME_CONTENT_SAMPLE AS
SELECT 
  relative_path,
  TO_VARCHAR(
    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
      '@CORTEX_SEARCH_DEMO.PEOPLE.INTERNAL_PEOPLE_STAGE',
      relative_path,
      {'mode':'LAYOUT'}
    ):content
  ) AS parsed_text
FROM SAMPLE_FILES;

SELECT COUNT(*) AS parsed_rows FROM PARSED_RESUME_CONTENT_SAMPLE;
SELECT relative_path, LEFT(parsed_text, 400) AS sample_text
FROM PARSED_RESUME_CONTENT_SAMPLE
LIMIT 5;

/* ============================================================================
   3) Chunk the sample text for semantic search
   ============================================================================ */
CREATE OR REPLACE TABLE CHUNKED_RESUME_CONTENT_SAMPLE (
  file_name VARCHAR,
  chunk     VARCHAR
);

INSERT INTO CHUNKED_RESUME_CONTENT_SAMPLE (file_name, chunk)
SELECT
  relative_path AS file_name,
  c.value::string AS chunk
FROM PARSED_RESUME_CONTENT_SAMPLE,
LATERAL FLATTEN(
  INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
    parsed_text,
    'markdown',
    1800,
    250
  )
) c;

SELECT COUNT(*) AS chunk_rows FROM CHUNKED_RESUME_CONTENT_SAMPLE;

/* ============================================================================
   4) NEW — Split chunks into lines (TEMP)
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE RESUME_LINES_SAMPLE AS
SELECT
  file_name,
  ROW_NUMBER() OVER (PARTITION BY file_name ORDER BY seq) - 1 AS line_no,
  line
FROM (
  SELECT
    file_name,
    t.index AS seq,
    t.value::string AS line
  FROM CHUNKED_RESUME_CONTENT_SAMPLE,
       LATERAL SPLIT_TO_TABLE(chunk, '\n') t
);

SELECT file_name, line_no, line
FROM RESUME_LINES_SAMPLE
LIMIT 10;

/* ============================================================================
   5) NEW — Detect CTO titles, score, and flag (per file)  [FIXED FOR SNOWFLAKE]
   - Use the 'i' parameter for case-insensitive regex (no inline (?i))
   ============================================================================ */
CREATE OR REPLACE TABLE RESUME_TITLE_SIGNALS_SAMPLE AS
WITH hits AS (
  SELECT
    file_name,
    line_no,
    line,
    -- Exact CTO hits (case-insensitive via 'i' parameter)
    REGEXP_LIKE(
      line,
      '\\b(CTO|Chief\\s+Technology\\s+Officer)\\b',
      'i'
    ) AS is_cto_exact,

    -- Related exec-technology titles
    REGEXP_LIKE(
      line,
      '\\b(Head\\s+of\\s+Technology|VP\\s+of\\s+Technology|VP\\s+of\\s+Engineering|Vice\\s+President\\s+of\\s+Technology|Technology\\s+Director|Director\\s+of\\s+Technology)\\b',
      'i'
    ) AS is_related_exec
  FROM RESUME_LINES_SAMPLE
)
SELECT
  file_name,

  -- Distinct matched titles (use 'i' param in REGEXP_SUBSTR too)
  ARRAY_AGG(DISTINCT
    CASE
      WHEN is_cto_exact THEN REGEXP_SUBSTR(
        line,
        '(CTO|Chief\\s+Technology\\s+Officer)',
        1, 1, 'i'
      )
      WHEN is_related_exec THEN REGEXP_SUBSTR(
        line,
        '(Head\\s+of\\s+Technology|VP\\s+of\\s+Technology|VP\\s+of\\s+Engineering|Vice\\s+President\\s+of\\s+Technology|Technology\\s+Director|Director\\s+of\\s+Technology)',
        1, 1, 'i'
      )
    END
  ) AS title_matches,

  -- Score: +3 exact CTO, +1 related, +2 if exact CTO appears near the top
  (3 * COUNT_IF(is_cto_exact))
  + (1 * COUNT_IF(is_related_exec))
  + (2 * MAX(IFF(is_cto_exact AND line_no <= 50, 1, 0))) AS title_score,

  -- Flag as CTO if score is strong enough
  (
    (3 * COUNT_IF(is_cto_exact))
    + (1 * COUNT_IF(is_related_exec))
    + (2 * MAX(IFF(is_cto_exact AND line_no <= 50, 1, 0)))
  ) >= 3 AS is_cto
FROM hits
GROUP BY file_name;

-- Inspect results
SELECT * FROM RESUME_TITLE_SIGNALS_SAMPLE
ORDER BY title_score DESC, file_name;


/* ============================================================================
   6) NEW — Enrich chunks with CTO attributes
   ============================================================================ */
CREATE OR REPLACE TABLE CHUNKED_RESUME_CONTENT_ENRICHED_SAMPLE AS
SELECT
  c.file_name,
  c.chunk,
  s.title_matches,
  s.title_score,
  s.is_cto
FROM CHUNKED_RESUME_CONTENT_SAMPLE c
JOIN RESUME_TITLE_SIGNALS_SAMPLE s USING (file_name);

SELECT COUNT(*) AS enriched_rows FROM CHUNKED_RESUME_CONTENT_ENRICHED_SAMPLE;

/* ============================================================================
   7) Build a SAMPLE Cortex Search Service
   - Uses smaller dataset → faster build + quick validation
   ============================================================================ */
CREATE OR REPLACE CORTEX SEARCH SERVICE RESUME_SEARCH_SERVICE_SAMPLE
  ON chunk
  WAREHOUSE = TRANSFORM_WH_XS
  TARGET_LAG = '90 minute'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS
SELECT
  file_name,
  chunk,
  title_matches,
  title_score,
  is_cto
FROM CHUNKED_RESUME_CONTENT_ENRICHED_SAMPLE;

-- (Optional) CTO-only SAMPLE service
CREATE OR REPLACE CORTEX SEARCH SERVICE CTO_RESUME_SEARCH_SAMPLE
  ON chunk
  WAREHOUSE = TRANSFORM_WH_XS
  TARGET_LAG = '90 minute'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS
SELECT file_name, chunk
FROM CHUNKED_RESUME_CONTENT_ENRICHED_SAMPLE
WHERE is_cto = TRUE;

SHOW CORTEX SEARCH SERVICES IN SCHEMA PEOPLE;

/* ============================================================================
   8) Preview queries (sample services)
   - If you get empty results immediately, wait a bit and rerun — indexing can take a moment
   ============================================================================ */

SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'CORTEX_SEARCH_DEMO.PEOPLE.RESUME_SEARCH_SERVICE_SAMPLE',
    '{
      "query": "CTO with cloud and security leadership",
      "columns": ["file_name","chunk","title_matches","title_score","is_cto"],
      "limit": 6
    }'
  )
)['results'] AS results;

