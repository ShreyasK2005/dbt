-- =========================
-- Context (adjust if needed)
-- =========================
USE ROLE TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH_XS;
USE DATABASE CORTEX_SEARCH_DEMO;
USE SCHEMA PEOPLE;

-- Source view (assumes PARSED_RESUME_CONTENT already exists)
CREATE OR REPLACE VIEW SOURCE_PARSED AS
SELECT relative_path AS file_name, parsed_text
FROM PARSED_RESUME_CONTENT;

-- (Optional) simple line splitter to show a few lines as context
CREATE OR REPLACE TEMP TABLE RESUME_LINES_SIMPLE AS
WITH u AS (
  SELECT file_name, REGEXP_REPLACE(parsed_text,'(\r\n|\r)','\n') AS t
  FROM SOURCE_PARSED
)
SELECT
  file_name,
  t.index AS line_no,
  REGEXP_REPLACE(TRIM(t.value::string),'[[:space:]]+',' ') AS line_norm
FROM u, LATERAL SPLIT_TO_TABLE(t, '\n') t
WHERE line_norm <> '';

-- 1) Ask the model for project recommendations (loose / intuitive)
--    Model: pick one you have enabled (arctic or a hosted model). 'snowflake-arctic' is a safe default.
CREATE OR REPLACE TEMP TABLE RESUME_PROJECT_RECS_AI_RAW AS
SELECT
  file_name,
  SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'You are a recruiting assistant. Read the resume and suggest 3–5 high-level project TYPES this person would likely excel at (e.g., "Cloud migration playbook", "Real-time data pipeline", "Security hardening initiative").
Return ONLY compact JSON with keys "projects" (array of short strings) and "why" (one short sentence).
No prose outside JSON.

RESUME TEXT:
' || LEFT(parsed_text, 15000)
  ) AS raw_output
FROM SOURCE_PARSED;

-- 2) Clean possible code fences and parse JSON
CREATE OR REPLACE TEMP TABLE RESUME_PROJECT_RECS_AI_CLEAN AS
SELECT
  file_name,
  -- Remove ```json ... ``` if the model added fences
  REGEXP_REPLACE(
    REGEXP_REPLACE(raw_output, '^```json\\s*', ''),
    '\\s*```\\s*$',
    ''
  ) AS cleaned
FROM RESUME_PROJECT_RECS_AI_RAW;

-- 3) Final table with arrays + a few example lines for context
CREATE OR REPLACE TABLE RESUME_PROJECT_RECS_AI AS
WITH parsed AS (
  SELECT
    file_name,
    TRY_PARSE_JSON(cleaned) AS rec
  FROM RESUME_PROJECT_RECS_AI_CLEAN
),
example_lines AS (
  SELECT file_name, ARRAY_AGG(line_norm) AS example_lines
  FROM (
    SELECT
      file_name,
      line_norm,
      ROW_NUMBER() OVER (PARTITION BY file_name ORDER BY line_no) AS rnk
    FROM RESUME_LINES_SIMPLE
  )
  WHERE rnk <= 3
  GROUP BY file_name
)
SELECT
  p.file_name,
  COALESCE(p.rec:"projects", ARRAY_CONSTRUCT())                AS recommended_projects,
  COALESCE(p.rec:"why"::string, '')                           AS rationale,
  COALESCE(e.example_lines, ARRAY_CONSTRUCT())                AS example_experience_lines
FROM parsed p
LEFT JOIN example_lines e USING (file_name)
ORDER BY p.file_name;

-- View results
SELECT * FROM RESUME_PROJECT_RECS_AI ORDER BY file_name;
