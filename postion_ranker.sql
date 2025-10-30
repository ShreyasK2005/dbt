/* =============================================================================
   Resume Quality Ranker (Snowflake-safe)
   - Preserves newlines before splitting (so headings like "Chief Technology Officer" are seen)
   - Uses Snowflake-safe regex (no (?i), no (?:...), position=1 then flags='i')
   - Uses SEQ4()+1 for REGEXP_SUBSTR occurrences (must be positive)
   - Correct CTAS pattern for VALUES (SELECT ... FROM VALUES)
   - Adds doc-wide title fallback + expanded skills lexicon
   Output: RESUME_QUALITY_RANKINGS
   =========================================================================== */

-- Workspace context (adjust if needed)
USE ROLE TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH_XS;
USE DATABASE CORTEX_SEARCH_DEMO;
USE SCHEMA PEOPLE;

-- -----------------------------------------------------------------------------
-- SOURCE (pick ONE). Default: full dataset. If you ran the SAMPLE flow, switch.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW SOURCE_PARSED AS
SELECT relative_path AS file_name, parsed_text FROM PARSED_RESUME_CONTENT;

-- -- SAMPLE version (uncomment this and comment the FULL view above)
-- CREATE OR REPLACE VIEW SOURCE_PARSED AS
-- SELECT relative_path AS file_name, parsed_text FROM PARSED_RESUME_CONTENT_SAMPLE;

/* ============================================================================
   1) Normalize & split (preserve newlines!)  [FIX: don't collapse \n away]
   - Convert CRLF/CR to LF; split on '\n'
   - Then normalize per-line whitespace with POSIX [[:space:]] (no \u escapes)
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE RESUME_LINES_ALL AS
WITH unified AS (
  SELECT
    file_name,
    REGEXP_REPLACE(parsed_text, '(\r\n|\r)', '\n') AS text_unified  -- keep line breaks
  FROM SOURCE_PARSED
)
SELECT
  file_name,
  t.index AS line_no,                                               -- 0-based
  TRIM(t.value::string) AS line_raw,
  REGEXP_REPLACE(TRIM(t.value::string), '[[:space:]]+', ' ') AS line_norm
FROM unified,
     LATERAL SPLIT_TO_TABLE(text_unified, '\n') t;

/* ============================================================================
   2) Title signals
   2A) Doc-wide fallback scan (helps if line-split misses headings)
   2B) Line-by-line scan + early-title bonus, merged with fallback
   ============================================================================ */

-- 2A) Document-level title fallback  [uses REGEXP_COUNT(subject, pattern, position, flags)]
CREATE OR REPLACE TEMP TABLE TITLE_DOC_SCAN AS
SELECT
  file_name,
  REGEXP_COUNT(parsed_text, '\\b(CTO|Chief\\s+Technology\\s+Officer)\\b', 1, 'i') AS cto_hits,
  REGEXP_COUNT(parsed_text, '\\b(CPO|Chief\\s+Product\\s+Officer)\\b',     1, 'i') AS cpo_hits,
  REGEXP_COUNT(parsed_text, '\\b(VP|Vice\\s+President)\\b',                1, 'i') AS vp_hits,
  REGEXP_COUNT(parsed_text, '\\b(Director|Head\\s+of)\\b',                 1, 'i') AS dir_hits,
  REGEXP_COUNT(parsed_text, '\\b(Principal|Staff)\\b',                     1, 'i') AS principal_hits,
  REGEXP_COUNT(parsed_text, '\\b(Senior|Lead)\\b',                         1, 'i') AS senior_hits,
  REGEXP_COUNT(parsed_text, '\\b(Manager|Architect)\\b',                   1, 'i') AS mgr_arch_hits
FROM SOURCE_PARSED;

-- 2B) Line-by-line titles + early-title bonus, merged with doc fallback
CREATE OR REPLACE TEMP TABLE TITLE_SIGNALS AS
WITH line_scores AS (
  SELECT
    file_name,
    8 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(CTO|Chief\\s+Technology\\s+Officer)\\b', 'i')) +
    7 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(CPO|Chief\\s+Product\\s+Officer)\\b', 'i')) +
    6 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(VP|Vice\\s+President)\\b', 'i')) +
    5 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(Director|Head\\s+of)\\b', 'i')) +
    4 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(Principal|Staff)\\b', 'i')) +
    3 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(Senior|Lead)\\b', 'i')) +
    2 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(Manager|Architect)\\b', 'i')) +
    1 * COUNT_IF(REGEXP_LIKE(line_norm, '\\b(Engineer|Developer|Scientist)\\b', 'i')) AS line_score,
    2 * MAX(IFF(
          line_no <= 50 AND REGEXP_LIKE(
            line_norm,
            '\\b(CTO|Chief\\s+Technology\\s+Officer|CPO|Chief\\s+Product\\s+Officer|VP|Vice\\s+President|Director|Head\\s+of|Principal|Staff|Lead)\\b',
            'i'
          ),
          1, 0
        )) AS early_title_bonus
  FROM RESUME_LINES_ALL
  GROUP BY file_name
)
SELECT
  COALESCE(l.file_name, d.file_name) AS file_name,
  /* Merge: line-based score + doc-wide hits as additive bonuses */
  COALESCE(l.line_score, 0)
    + 8 * COALESCE(d.cto_hits, 0)
    + 7 * COALESCE(d.cpo_hits, 0)
    + 6 * COALESCE(d.vp_hits, 0)
    + 5 * COALESCE(d.dir_hits, 0)
    + 4 * COALESCE(d.principal_hits, 0)
    + 2 * COALESCE(d.mgr_arch_hits, 0)
    + 3 * COALESCE(d.senior_hits, 0) AS seniority_score,
  COALESCE(l.early_title_bonus, 0)   AS early_title_bonus
FROM line_scores l
FULL OUTER JOIN TITLE_DOC_SCAN d USING (file_name);

/* ============================================================================
   3) Leadership / ownership verbs
   [FIX: REGEXP_COUNT requires position before flags → 1, 'i']
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE LEADERSHIP_SIGNALS AS
SELECT
  p.file_name,
  REGEXP_COUNT(
    p.parsed_text,
    '\\b(led|managed|mentored|hired|grew|scaled|owned|built|established|architected)\\b',
    1, 'i'
  ) AS leadership_hits
FROM SOURCE_PARSED p;

/* ============================================================================
   4) Experience years (heuristic)  [FIX: SEQ4()+1 for occurrence]
   - explicit: “… (X) years …”  (captures group #1 = X)
   - range   : earliest to latest 19xx/20xx → inclusive years
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE YEAR_MENTIONS AS
SELECT
  p.file_name,
  MAX(TO_NUMBER(
        REGEXP_SUBSTR(
          p.parsed_text,
          '(\\d{1,2})\\s*\\+?\\s*years',
          1,                -- position
          SEQ4()+1,         -- occurrence (must be positive)
          'i',              -- flags
          1                 -- return capture group #1
        )
      )
  ) AS explicit_years
FROM SOURCE_PARSED p,
     TABLE(GENERATOR(ROWCOUNT => 100))
GROUP BY p.file_name;

CREATE OR REPLACE TEMP TABLE YEAR_RANGE AS
SELECT
  p.file_name,
  MIN(TO_NUMBER(
        REGEXP_SUBSTR(
          p.parsed_text,
          '(19|20)\\d{2}',
          1,
          SEQ4()+1
        )
      )
  ) AS first_year,
  MAX(TO_NUMBER(
        REGEXP_SUBSTR(
          p.parsed_text,
          '(19|20)\\d{2}',
          1,
          SEQ4()+1
        )
      )
  ) AS last_year
FROM SOURCE_PARSED p,
     TABLE(GENERATOR(ROWCOUNT => 200))
GROUP BY p.file_name;

CREATE OR REPLACE TEMP TABLE EXPERIENCE_YEARS AS
SELECT
  y.file_name,
  NVL(y.explicit_years, 0) AS explicit_years,
  IFF(r.first_year IS NULL OR r.last_year IS NULL OR r.last_year < r.first_year,
      0, r.last_year - r.first_year + 1) AS range_years,
  GREATEST(
    NVL(y.explicit_years, 0),
    IFF(r.first_year IS NULL OR r.last_year IS NULL OR r.last_year < r.first_year, 0, r.last_year - r.first_year + 1)
  ) AS experience_years_est
FROM YEAR_MENTIONS y
LEFT JOIN YEAR_RANGE r USING (file_name);

/* ============================================================================
   5) Skills lexicon (expanded) + matches per resume
   [FIX: CTAS from VALUES must be SELECT ... FROM VALUES; escape '.' for Node.js]
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE SKILL_LEXICON AS
SELECT column1::varchar AS skill, column2::float AS weight
FROM VALUES
  -- Cloud & services
  ('AWS',2.0),('GCP',1.6),('Azure',1.6),
  ('S3',1.0),('Lambda',1.0),('EC2',0.8),('EKS',1.0),('GKE',1.0),('AKS',1.0),
  ('CloudWatch',0.7),('DynamoDB',1.0),('RDS',0.8),
  -- Data/Warehousing
  ('Snowflake',2.0),('Redshift',1.4),('BigQuery',1.4),('Databricks',1.6),('dbt',1.2),
  -- Streaming / Pipelines
  ('Kafka',1.6),('Flink',1.2),('Airflow',1.2),('Spark',1.2),
  -- Containers / Infra
  ('Kubernetes',2.0),('K8s',1.6),('Docker',1.2),('Terraform',1.2),('Ansible',0.9),
  -- App / Backend
  ('Node.js',1.0),('Go',1.2),('Python',1.3),('Java',1.1),('Rust',1.5),
  -- Frontend
  ('React',0.9),('Angular',0.7),
  -- Security & Compliance
  ('Security',1.2),('Cybersecurity',1.6),('Zero Trust',1.0),
  ('SOC2',1.2),('HIPAA',1.2),('PCI',1.0),('OAuth2',0.8),('JWT',0.8),
  -- ML / AI
  ('Machine Learning',1.6),('ML',1.2),('NLP',1.1),('LLM',1.3),
  ('TensorFlow',1.0),('PyTorch',1.2),('scikit-learn',0.9),('MLflow',0.8),
  -- LLM/dev tooling
  ('LangChain',1.1),('Vector DB',1.1),('Pinecone',1.1),('Milvus',1.1),('Weaviate',1.0),('FAISS',1.0),
  ('HuggingFace',1.0),('OpenAI',1.0),
  -- Databases
  ('PostgreSQL',1.0),('Postgres',1.0),('MySQL',0.9),('MongoDB',1.0),('Redis',0.8);

CREATE OR REPLACE TEMP TABLE SKILL_LEXICON_RX AS
SELECT
  skill,
  weight,
  REPLACE(skill, '.', '\\.') AS skill_rx
FROM SKILL_LEXICON;

CREATE OR REPLACE TEMP TABLE SKILL_MATCHES AS
SELECT
  p.file_name,
  ARRAY_AGG(DISTINCT s.skill) AS skills_found,
  COUNT(DISTINCT s.skill)     AS skills_count,
  SUM(DISTINCT s.weight)      AS skills_weight
FROM SOURCE_PARSED p
JOIN SKILL_LEXICON_RX s
  ON REGEXP_LIKE(p.parsed_text, CONCAT('(^|\\W)(', s.skill_rx, ')(\\W|$)'), 'i')
GROUP BY p.file_name;

/* ============================================================================
   6) Impact signals (money, %, scale)
   [FIX: no (?:...), use capturing groups; position=1, flags='i'; allow decimals]
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE IMPACT_SIGNALS AS
SELECT
  p.file_name,
  -- $ amounts: $250,000 | $ 2M | $3.5B
  REGEXP_COUNT(
    p.parsed_text,
    '\\$[[:space:]]?\\d[\\d,]*(\\.\\d+)?([[:space:]]?(K|M|B))?',
    1, 'i'
  ) AS money_hits,
  -- percentages: 45% | 7 % | 12.5%
  REGEXP_COUNT(
    p.parsed_text,
    '\\b\\d+(\\.\\d+)?[[:space:]]?%\\b',
    1, 'i'
  ) AS percent_hits,
  -- scale tokens: 10K | 5M | 1B | 2.5M
  REGEXP_COUNT(
    p.parsed_text,
    '\\b\\d+(\\.\\d+)?(K|M|B)\\b',
    1, 'i'
  ) AS scale_hits
FROM SOURCE_PARSED p;

/* ============================================================================
   7) Education / certs
   [FIX: cert_hits must be aggregated in GROUP BY select]
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE EDU_CERT_SIGNALS AS
SELECT
  p.file_name,
  2 * COUNT_IF(REGEXP_LIKE(p.parsed_text, '\\b(PhD|Doctor\\s+of)\\b', 'i')) +
  1 * COUNT_IF(REGEXP_LIKE(p.parsed_text, '\\b(Master\\s+of|MSc|MS\\s+in|MBA)\\b', 'i')) AS education_score,
  SUM(
    REGEXP_COUNT(
      p.parsed_text,
      '\\b(AWS\\s+Certified|CISSP|PMP|GCP\\s+Professional|Azure\\s+Administrator|CKA)\\b',
      1, 'i'
    )
  ) AS cert_hits
FROM SOURCE_PARSED p
GROUP BY p.file_name;

/* ============================================================================
   8) Assemble features per resume
   ============================================================================ */
CREATE OR REPLACE TEMP TABLE RESUME_FEATURES AS
SELECT
  p.file_name,
  (t.seniority_score + t.early_title_bonus) AS seniority_points,
  l.leadership_hits,
  e.experience_years_est,
  COALESCE(s.skills_count, 0)    AS skills_count,
  COALESCE(s.skills_weight, 0.0) AS skills_weight,
  COALESCE(s.skills_found, ARRAY_CONSTRUCT()) AS skills_found,
  i.money_hits, i.percent_hits, i.scale_hits,
  ec.education_score, ec.cert_hits,
  LEFT(p.parsed_text, 400) AS preview_text
FROM SOURCE_PARSED p
LEFT JOIN TITLE_SIGNALS       t  USING (file_name)
LEFT JOIN LEADERSHIP_SIGNALS  l  USING (file_name)
LEFT JOIN EXPERIENCE_YEARS    e  USING (file_name)
LEFT JOIN SKILL_MATCHES       s  USING (file_name)
LEFT JOIN IMPACT_SIGNALS      i  USING (file_name)
LEFT JOIN EDU_CERT_SIGNALS    ec USING (file_name);

/* ============================================================================
   9) Normalize 0..1 and compute composite score
   - Protects against division by zero when min=max
   ============================================================================ */
CREATE OR REPLACE TABLE RESUME_QUALITY_RANKINGS AS
WITH stats AS (
  SELECT
    MIN(seniority_points) AS mn_sen, MAX(seniority_points) AS mx_sen,
    MIN(leadership_hits)  AS mn_lead, MAX(leadership_hits)  AS mx_lead,
    MIN(experience_years_est) AS mn_exp, MAX(experience_years_est) AS mx_exp,
    MIN(skills_weight)    AS mn_skw, MAX(skills_weight)    AS mx_skw,
    MIN(money_hits + percent_hits + scale_hits) AS mn_imp,
    MAX(money_hits + percent_hits + scale_hits) AS mx_imp,
    MIN(education_score + cert_hits) AS mn_edu, MAX(education_score + cert_hits) AS mx_edu
  FROM RESUME_FEATURES
),
normed AS (
  SELECT
    f.*,
    IFF(mx_sen = mn_sen, 0, (seniority_points - mn_sen)/(mx_sen - mn_sen)) AS n_seniority,
    IFF(mx_lead = mn_lead, 0, (leadership_hits - mn_lead)/(mx_lead - mn_lead)) AS n_leadership,
    IFF(mx_exp = mn_exp, 0, (experience_years_est - mn_exp)/(mx_exp - mn_exp)) AS n_experience,
    IFF(mx_skw = mn_skw, 0, (skills_weight - mn_skw)/(mx_skw - mn_skw)) AS n_skills,
    IFF(mx_imp = mn_imp, 0, ((money_hits + percent_hits + scale_hits) - mn_imp)/(mx_imp - mn_imp)) AS n_impact,
    IFF(mx_edu = mn_edu, 0, ((education_score + cert_hits) - mn_edu)/(mx_edu - mn_edu)) AS n_edu
  FROM RESUME_FEATURES f, stats
)
SELECT
  file_name AS resume_file,
  seniority_points, leadership_hits, experience_years_est,
  skills_count, skills_weight, skills_found,
  money_hits, percent_hits, scale_hits,
  education_score, cert_hits,
  preview_text,
  n_seniority, n_leadership, n_experience, n_skills, n_impact, n_edu,
  ROUND(
    100 * (
      0.30 * n_seniority +
      0.20 * n_experience +
      0.20 * n_skills +
      0.15 * n_impact +
      0.10 * n_leadership +
      0.05 * n_edu
    ), 1
  ) AS quality_score,
  DENSE_RANK() OVER (
    ORDER BY quality_score DESC, n_seniority DESC, n_impact DESC, resume_file ASC
  ) AS quality_rank
FROM normed
ORDER BY quality_rank;

/* Quick view */
SELECT *
FROM RESUME_QUALITY_RANKINGS
ORDER BY quality_rank;
