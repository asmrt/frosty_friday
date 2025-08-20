-- =========================================================
--  メモ化（MEMOIZABLE）あり/なしのA/B比較・ワンショット実行スクリプト
--  目的：
--    1) 同一引数が多いデータセット上で UDF のメモ化効果を観察
--    2) 同一クエリを2回連続実行し、2回目での短縮を確認
--    3) MEMOIZABLEなしUDFと比較（同条件）
--  注意：
--    - 結果キャッシュは無効化（UDFメモ化そのものの効果を見たい）
--    - データキャッシュ等の影響を完全排除はできない点は留意
-- =========================================================

-- [0] 結果キャッシュ無効化（UDFメモ化だけを観測しやすくする）
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- [0-1] （任意）確認用：現在のセッション設定
SELECT 'USE_CACHED_RESULT' AS param, CURRENT_SESSION()::variant:"parameters":"USE_CACHED_RESULT"::string AS value;

-- [0-2]事前定義した「メモ化ありUDF」の存在を確認
-- 定義したUDFの確認
-- 　想定ファンクション名：validate_patent_gap_with_memoizable
SHOW USER FUNCTIONS IN SCHEMA FROSTY_FRIDAY_DB.WEEK_65_SCHEMA;

-- [1] 準備：比較対象の「メモ化なしUDF」を作成（ロジックは同じ）
--     既に存在する場合は REPLACE で上書き
CREATE OR REPLACE FUNCTION FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.VALIDATE_PATENT_GAP_NO_MEMOIZABLE(
  application_date DATE, publication_date DATE, patent_type STRING
)
RETURNS BOOLEAN
AS
$$
CASE
  WHEN application_date IS NULL OR publication_date IS NULL THEN FALSE
  WHEN publication_date < application_date THEN FALSE
  WHEN TRIM(LOWER(patent_type)) = 'reissue' AND DATEDIFF(DAY, application_date, publication_date) <= 365 THEN TRUE
  WHEN TRIM(LOWER(patent_type)) = 'design'  AND DATEDIFF(DAY, application_date, publication_date) <= 730 THEN TRUE
  ELSE FALSE
END
$$;
-- 定義したUDFの確認
SHOW USER FUNCTIONS IN SCHEMA FROSTY_FRIDAY_DB.WEEK_65_SCHEMA;


-- [2] ベンチ用データ作成
--     ・大量の“同一引数（ホットキー）”を含め、メモ化ヒットが起きやすい状況を作る
--     ・少量の“コールド（ほぼユニーク）”も混ぜ、現実っぽさを追加
-- 参考マニュアル：
-- TEMP TABLE：https://docs.snowflake.com/ja/user-guide/tables-temp-transient#temporary-tables

-- A）ホットキー多めテーブル
CREATE OR REPLACE TEMP TABLE memo_bench_hot AS
SELECT * FROM (
  SELECT TO_DATE('2020-01-01') AS application_date, TO_DATE('2020-12-31') AS publication_date, 'reissue' AS patent_type FROM TABLE(GENERATOR(ROWCOUNT=>400000))
  UNION ALL
  SELECT TO_DATE('2021-04-15'), TO_DATE('2023-04-14'), 'design'  FROM TABLE(GENERATOR(ROWCOUNT=>400000))
  UNION ALL
  SELECT TO_DATE('2024-01-01'), TO_DATE('2024-01-02'), 'utility' FROM TABLE(GENERATOR(ROWCOUNT=>200000))
);
-- 作成データ確認
-- 想定のホットキーが多いテーブルになっているか確認する。
SELECT
    application_date,
    publication_date,
    patent_type,
    COUNT(*) AS cnt
FROM
    memo_bench_hot
GROUP BY ALL
ORDER BY 1,2
;
-- B）コールドキー多めテーブル
CREATE OR REPLACE TEMP TABLE memo_bench_cold AS
SELECT
  DATEADD('day', UNIFORM(-1200, 1200, RANDOM()), '2022-01-01') AS application_date,
  DATEADD('day', UNIFORM(-1200, 1200, RANDOM()), '2022-01-01') AS publication_date,
  CASE MOD(SEQ4(), 5)
    WHEN 0 THEN 'reissue'
    WHEN 1 THEN 'design'
    WHEN 2 THEN 'utility'
    WHEN 3 THEN ' ReIssue '
    ELSE       ' DESIGN  '
  END AS patent_type
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

-- 作成データ確認
-- 想定のコールドキーが多いテーブルになっているか確認する。
SELECT
    application_date,
    publication_date,
    patent_type,
    COUNT(*) AS cnt
FROM
    memo_bench_cold
GROUP BY ALL
ORDER BY 1,2
;
CREATE OR REPLACE TEMP TABLE memo_bench AS
SELECT * FROM memo_bench_hot
UNION ALL
SELECT * FROM memo_bench_cold;

-- データ確認
SELECT
    application_date,
    publication_date,
    patent_type,
    COUNT(*) AS cnt
FROM
    memo_bench
GROUP BY ALL
HAVING cnt > 5
ORDER BY 1,2
;

-- [2-1] データの重複度（＝メモ化ヒット上限の目安）を把握
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT (application_date, publication_date, TRIM(LOWER(patent_type)))) AS unique_arg_tuples
FROM memo_bench;

-- [3] Aパート：MEMOIZABLE ありUDFの2回実行（ウォーム→本番）
--     クエリタグで run を識別し、直後に QUERY_HISTORY から経過時間（ミリ秒）を取得
-- 参考マニュアル
-- 　QUERY_HISTORY /QUERY_HISTORY_BY_*：https://docs.snowflake.com/ja/sql-reference/functions/query_history
ALTER SESSION SET QUERY_TAG = 'A_MEMO_WARMUP_RUN1';
SELECT
    COUNT_IF(
        FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(
            application_date,
            publication_date,
            patent_type
        )
) AS positives
FROM
    memo_bench;

-- 実行時間（1回目）
SELECT
    QUERY_ID,
    QUERY_TYPE,
    START_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TAG = 'A_MEMO_WARMUP_RUN1'
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY START_TIME DESC
;
-- 2回目（同一クエリ；メモ化キャッシュヒット期待）
ALTER SESSION SET QUERY_TAG = 'A_MEMO_MEASURE_RUN2';
SELECT
    COUNT_IF(
    FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(
        application_date,
        publication_date,
        patent_type
    )
) AS positives
FROM
    memo_bench
;

-- 実行時間（2回目）
SELECT
    QUERY_ID,
    QUERY_TYPE,
    START_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TAG = 'A_MEMO_MEASURE_RUN2'
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY
    START_TIME DESC
;
-- [4] Bパート：MEMOIZABLE なしUDFの2回実行（ウォーム→本番）
--     同じデータ・同じ集計で比較。2回目短縮は相対的に小さい想定
ALTER SESSION SET QUERY_TAG = 'B_NOMEMO_WARMUP_RUN1';
SELECT
    COUNT_IF(
        FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.VALIDATE_PATENT_GAP_NO_MEMOIZABLE(
            application_date,
            publication_date,
            patent_type
    )
) AS positives
FROM
    memo_bench
;

-- 実行時間（1回目）
SELECT
    QUERY_ID,
    QUERY_TYPE,
    START_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TAG = 'B_NOMEMO_WARMUP_RUN1'
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY
    START_TIME DESC
;
-- 2回目（同一クエリ）
ALTER SESSION SET QUERY_TAG = 'B_NOMEMO_MEASURE_RUN2';
SELECT
    COUNT_IF(
        FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.VALIDATE_PATENT_GAP_NO_MEMOIZABLE(
            application_date,
            publication_date,
            patent_type
    )
) AS positives
FROM
    memo_bench
;
-- 実行時間（2回目）
SELECT
    QUERY_ID,
    QUERY_TYPE,
    START_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TAG = 'B_NOMEMO_MEASURE_RUN2'
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY
    START_TIME DESC
;
-- [5] 参考：代表ホットキー3パターンの出現回数（どの程度“同一引数”が多いか）
SELECT
  application_date, publication_date, TRIM(LOWER(patent_type)) AS norm_type,
  COUNT(*) AS cnt
FROM memo_bench
WHERE (application_date, publication_date, TRIM(LOWER(patent_type))) IN (
  (TO_DATE('2020-01-01'), TO_DATE('2020-12-31'), 'reissue'),
  (TO_DATE('2021-04-15'), TO_DATE('2023-04-14'), 'design'),
  (TO_DATE('2024-01-01'), TO_DATE('2024-01-02'), 'utility')
)
GROUP BY 1,2,3
ORDER BY cnt DESC;

-- [6] まとめ：今回の4つのクエリタグの直近1件ずつを横並びサマリ
ALTER SESSION SET QUERY_TAG = 'RUN_SUMMARY';
SELECT
    QUERY_TAG,
    QUERY_ID,
    QUERY_TYPE,
    START_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TAG IN('A_MEMO_WARMUP_RUN1','A_MEMO_MEASURE_RUN2','B_NOMEMO_WARMUP_RUN1','B_NOMEMO_MEASURE_RUN2')
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY
    START_TIME DESC
;
-- account_usage.QUERY_HISTORY でキャッシュ効果を確認

-- 　 注意点：ACCOUNT_USAGE.QUERY_HISTORYの場合、最大45分の遅延が発生する。

-- → メモ化のUDFが呼び出されキャッシュが利用されると、child_queries_wait_time(クエリの子ジョブを完了するためのミリ秒数)カラムの数値が、0より大きくなる想定
--   → 今回のケースではすべて0になってしまう。キャッシュが効いていない可能性がありそう。
--      → マニュアル内のスカラSQL UDF欄において、以下記載あり。引数が変数（例：application_date）のため、
--        キャッシュが効かない可能性があると想定（要確認）
--   　 「引数を指定する場合、引数は以下のデータ型のいずれかの定数値である必要があります。」
--      
--      Next Action：定数を指定してみるとキャッシュが効くのか？
SELECT
    query_id,
    query_tag,
    start_time,
    total_elapsed_time,
    child_queries_wait_time,
    query_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  AND QUERY_TAG IN('A_MEMO_WARMUP_RUN1','A_MEMO_MEASURE_RUN2','B_NOMEMO_WARMUP_RUN1','B_NOMEMO_MEASURE_RUN2')
  AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
  AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
  ORDER BY start_time DESC
  ;

--  Next Action：定数を指定してみるとキャッシュが効くのか？
--  検証方法
--    今回利用のアメリカ特許のデータを模したデータを作り、MEMOIZABLE 付き/なしの2種類のUDFを定義して呼び出し、
--    同じ引数で2回実行したときにキャッシュが効くかを ACCOUNT_USAGE.QUERY_HISTORY で確認する

-- スキーマ作成
CREATE OR REPLACE DATABASE MEMOIZE_TEST;
CREATE OR REPLACE SCHEMA MEMOIZE_TEST.PUBLIC;
USE SCHEMA MEMOIZE_TEST.PUBLIC;

-- テーブル作成
CREATE OR REPLACE TABLE uspto_contributor_index (
    contributor_id VARCHAR PRIMARY KEY,
    contributor_name VARCHAR,
    contributor_type VARCHAR,
    country VARCHAR
);

CREATE OR REPLACE TABLE uspto_patent_index (
    patent_id VARCHAR PRIMARY KEY,
    invention_title VARCHAR,
    patent_type VARCHAR,
    application_date DATE,
    document_publication_date DATE
);

CREATE OR REPLACE TABLE uspto_patent_contributor_relationships (
    patent_id VARCHAR,
    contributor_id VARCHAR,
    contribution_type VARCHAR
);

-- データ挿入
INSERT INTO uspto_contributor_index (contributor_id, contributor_name, contributor_type, country) VALUES
  ('C1', 'NVIDIA CORPORATION', 'Organization', 'US'),
  ('C2', 'ANOTHER TECH INC.', 'Organization', 'US');

INSERT INTO uspto_patent_index (patent_id, invention_title, patent_type, application_date, document_publication_date) VALUES
  ('P1', 'GPU-based parallel processing architecture', 'Utility', '2015-05-01', '2017-08-01'),
  ('P2', 'Graphics rendering method for neural networks', 'Utility', '2016-04-15', '2018-01-10'),
  ('P3', 'Quantum computing device with improved qubit connectivity', 'Utility', '2019-11-20', '2021-03-03');

INSERT INTO uspto_patent_contributor_relationships (patent_id, contributor_id, contribution_type) VALUES
  ('P1', 'C1', 'Assignee - United States Company Or Corporation'),
  ('P2', 'C1', 'Assignee - United States Company Or Corporation'),
  ('P3', 'C2', 'Assignee - United States Company Or Corporation');

-- 非Memoizable UDF
CREATE OR REPLACE FUNCTION get_patent_titles_no_cache(company_name VARCHAR)
RETURNS ARRAY
AS
$$
  SELECT ARRAY_AGG(p.invention_title)
    FROM uspto_contributor_index AS c
    JOIN uspto_patent_contributor_relationships AS r
         ON c.contributor_id = r.contributor_id
    JOIN uspto_patent_index AS p
         ON r.patent_id = p.patent_id
   WHERE c.contributor_name ILIKE company_name
     AND r.contribution_type = 'Assignee - United States Company Or Corporation'
$$;

-- Memoizable UDF
CREATE OR REPLACE FUNCTION get_patent_titles(company_name VARCHAR)
RETURNS ARRAY
MEMOIZABLE
AS
$$
  SELECT ARRAY_AGG(p.invention_title)
    FROM uspto_contributor_index AS c
    JOIN uspto_patent_contributor_relationships AS r
         ON c.contributor_id = r.contributor_id
    JOIN uspto_patent_index AS p
         ON r.patent_id = p.patent_id
   WHERE c.contributor_name ILIKE company_name
     AND r.contribution_type = 'Assignee - United States Company Or Corporation'
$$;

-- 結合クエリ（NVIDIAの特許情報を取得）
SELECT patent_index.patent_id,
       invention_title,
       patent_type,
       application_date,
       document_publication_date
  FROM uspto_contributor_index AS contributor_index
 INNER JOIN uspto_patent_contributor_relationships AS relationships
         ON contributor_index.contributor_id = relationships.contributor_id
 INNER JOIN uspto_patent_index AS patent_index
         ON relationships.patent_id = patent_index.patent_id
 WHERE contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
   AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
 LIMIT 10;

-- Memoizable UDF の呼び出し（1回目：キャッシュ生成）
SELECT get_patent_titles('NVIDIA CORPORATION') AS titles_from_memoizable;

-- Memoizable UDF の呼び出し（2回目：キャッシュ利用）
SELECT get_patent_titles('NVIDIA CORPORATION') AS titles_from_memoizable;

-- 非Memoizable UDF の呼び出し（1回目：キャッシュ生成）
SELECT get_patent_titles_no_cache('NVIDIA CORPORATION') AS titles_from_non_memoizable;

-- 非Memoizable UDF の呼び出し（2回目：キャッシュ利用）
SELECT get_patent_titles_no_cache('NVIDIA CORPORATION') AS titles_from_non_memoizable;

-- ACCOUNT_USAGE.QUERY_HISTORY でキャッシュ効果を確認
-- 　 注意点：ACCOUNT_USAGE.QUERY_HISTORYの場合、最大45分の遅延が発生する。
--  確認結果：以下状況のため、Memoizable UDF はキャッシュが効いているように見受けられる。
--            1. Memoizable UDFの方のみ、child_queries_wait_timee(クエリの子ジョブを完了するためのミリ秒数)の値が、0より大きい。
--            2. total_elapsed_time(経過時間（ミリ秒単位）)も非Memoizable UDF より短い。
SELECT
    query_id,
    start_time,
    total_elapsed_time,
    child_queries_wait_time,
    query_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  AND query_text ILIKE '%get_patent_titles(''NVIDIA CORPORATION'')%'
ORDER BY start_time DESC
;
