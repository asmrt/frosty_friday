-- 応用：SQL UDFに関するオプションの利用（MEMORAIZE）

-- 前提
--  以下week_65に関する以下クエリを実行済みの想定
--  https://github.com/asmrt/frosty_friday/blob/main/week_65/week_65_solution.sql
--  MEMORAIZEオプションを付けると、UDFを呼び出した結果をキャッシュし、同一の引数・返却値の場合に処理時間を短縮する。
-- SQLUDSQL Scaler Memoizable UDFs：https://docs.snowflake.com/en/developer-guide/udf/sql/udf-sql-scalar-functions#label-udf-sql-scalar-memoizable
-- 
-- SQL UDF作成/MEMOIZABLE付き

CREATE OR REPLACE FUNCTION FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(
    application_date DATE,
    publication_date DATE,
    patent_type STRING
)
RETURNS BOOLEAN
MEMOIZABLE
COMMENT = 'アメリカ特許：有効期間判定（MEMOIZABLE(メモ化)有）'
AS
$$
CASE
    WHEN application_date IS NULL OR publication_date IS NULL THEN FALSE
    WHEN publication_date < application_date THEN FALSE
    WHEN TRIM(LOWER(patent_type)) = 'reissue' AND DATEDIFF(DAY, application_date, publication_date) <= 365 THEN TRUE
    WHEN TRIM(LOWER(patent_type)) = 'design' AND DATEDIFF(DAY, application_date, publication_date) <= 730 THEN TRUE
    ELSE FALSE
END
$$
;
-- 3-1.UDFを使ってみる/特許タイプ：Design
-- 利用するUDFをメモ化(memoizable)したものに変更
-- 　変更前：validate_patent_gap_no_memoizable
-- 　変更後：validate_patent_gap_with_memoizable
--   →　正常に実行はできる。キャッシュが効くかの検証は、別クエリ「memoizable_test_query.sql」にて。
--       https://github.com/asmrt/frosty_friday/blob/main/week_65/memoizable_test_query.sql
SELECT patent_index.patent_id
    , invention_title
    , patent_type
    , application_date 
    , document_publication_date
    ,FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(application_date, document_publication_date, patent_type) AS validate_date_check -- 今回のUDFの実行結果(TRUE/FALSEを返却)
    , DATEDIFF(DAY , application_date, document_publication_date) AS datediff_for_udf_check -- 【テスト用】UDFの判定に誤りがないかチェックするカラム
FROM
    cybersyn_us_patent_grants.cybersyn.uspto_contributor_index AS contributor_index
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_contributor_relationships AS relationships
    ON contributor_index.contributor_id = relationships.contributor_id
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index AS patent_index
    ON relationships.patent_id = patent_index.patent_id
WHERE
    contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
    AND patent_type = 'Design'
    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(application_date, document_publication_date, patent_type) -- TRUEの場合をチェックする用の条件
    --AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable(application_date, document_publication_date, patent_type) = FALSE -- FALSEの場合をチェックする用の条件
LIMIT 10
;

-- UDF作成はうまくいくが、UDFの実行でエラーになるケース
-- <発生条件>
--    1.MEMOIZABLEオプションを付ける
--    2.実行する処理($$～$$内)にSELECT句を入れる
CREATE OR REPLACE FUNCTION FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable_error(
    application_date DATE,
    publication_date DATE,
    patent_type STRING
)
RETURNS BOOLEAN
MEMOIZABLE
COMMENT = 'アメリカ特許：有効期間判定（MEMOIZABLE(メモ化)有）/SELECT句 有でUDF実行エラー'
AS
$$
SELECT
CASE
    WHEN application_date IS NULL OR publication_date IS NULL THEN FALSE
    WHEN publication_date < application_date THEN FALSE
    WHEN TRIM(LOWER(patent_type)) = 'reissue' AND DATEDIFF(DAY, application_date, publication_date) <= 365 THEN TRUE
    WHEN TRIM(LOWER(patent_type)) = 'design' AND DATEDIFF(DAY, application_date, publication_date) <= 730 THEN TRUE
    ELSE FALSE
END
$$;

-- 3-1.UDFを使ってみる/特許タイプ：Design / エラー発生バージョン
--     エラー発生する想定のUDF「validate_patent_gap_with_memoizable_error」を呼び出し、以下エラーが発生することを確認する。
--       想定エラー：argument 0 to function VALIDATE_PATENT_GAP_WITH_MEMOIZABLE_ERROR needs to be constant, found 'PATENT_INDEX.APPLICATION_DATE'
SELECT patent_index.patent_id
    , invention_title
    , patent_type
    , application_date 
    , document_publication_date
    , FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable_error(application_date, document_publication_date, patent_type) AS validate_date_check -- 今回のUDFの実行結果(TRUE/FALSEを返却)
    , DATEDIFF(DAY , application_date, document_publication_date) AS datediff_for_udf_check -- 【テスト用】UDFの判定に誤りがないかチェックするカラム
FROM
    cybersyn_us_patent_grants.cybersyn.uspto_contributor_index AS contributor_index
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_contributor_relationships AS relationships
    ON contributor_index.contributor_id = relationships.contributor_id
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index AS patent_index
    ON relationships.patent_id = patent_index.patent_id
WHERE
    contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
    AND patent_type = 'Design'
    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_with_memoizable_error(application_date, document_publication_date, patent_type) -- TRUEの場合をチェックする用の条件
LIMIT 10
;

