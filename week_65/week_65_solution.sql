-- Week 65 – Basic (UDF & Marketplace)
-- https://frostyfriday.org/blog/2023/09/29/week-65-basic/

-- <問題 日本語訳>
-- 
-- 　今週は、マーケットプレイスにある 「Cybersyn US Patent Grants」 のデータセットを題材にして、それを自分たちの関数を使って操作してみましょう。
-- 　ユーザー定義関数（UDF）の良いところは、「自分専用のツールキット」みたいに使えることです。ある計算処理を保存しておけばチーム全体で共有できるし、
-- 　ビジネス要件が変わったときにも中央で一括修正できます。そして嬉しいことに、こうした関数はCybersynのような公開データセットにも使えるんです。
-- 
-- 　今回の流れ：
--   　1.マーケットプレイスから Cybersyn US Patent Grants のデータセットを取得する。
--   　2．CybersynのSQLクエリを使って、Nvidiaに関連する特許を検索する（結果が多すぎないように上限10件にしています）。
--   　3．以下のルールに基づいて、特許に「合格（TRUE）/不合格（FALSE）」を返す関数を作る：
--  　    <ルール>
--          申請日(APPLICATION_DATE) と発行日(PUBLICATION_DATE)の差が、以下に該当する場合はTRUE、それ以外はFALSEを返却する。
--  　　        Reissue（再発行）特許 → APPLICATION_DATE(申請) と PUBLICATION_DATE(発行日) の差が 365日以内
--  　　        Design（意匠）特許    → APPLICATION_DATE と PUBLICATION_DATE の差が2年(730日)以内

-- 0.事前準備：各種設定、DB、スキーマ作成
USE ROLE SYSADMIN;
USE warehouse COMPUTE_WH;

CREATE OR REPLACE DATABASE FROSTY_FRIDAY_DB;
CREATE OR REPLACE SCHEMA   WEEK_65_SCHEMA;

-- 1-1. MarketPlaceからデータ取得する
--    マーケットプレイスから Cybersyn US Patent Grants データセット（アメリカの特許に関する公開データセット）を取得する。
--    Cybersyn の SQL クエリを使用して、Nvidia に関連するすべての特許を発掘する (結果が多すぎて混乱しないよう、10 件に制限している)。
--      Marketplace  > 「US Patent」で検索
--      
--      ・対象データに直接アクセスできるURL：
--         https://app.snowflake.com/marketplace/listing/GZTSZ290BUX19/snowflake-public-data-products-us-patents

-- 1-2. 取得したMarketPlaceのデータの確認
-- 　　 マーケットプレイスからデータ取得した際にデフォルト表示されるクエリを実行し、どのようなデータがあるか確認する。

--    NVIDIA が指定された譲受人（Assignee）となっている特許をすべて取得する。
SELECT patent_index.patent_id
    , invention_title
    , patent_type
    , application_date 
    , document_publication_date
FROM cybersyn_us_patent_grants.cybersyn.uspto_contributor_index AS contributor_index
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_contributor_relationships AS relationships
    ON contributor_index.contributor_id = relationships.contributor_id
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index AS patent_index
    ON relationships.patent_id = patent_index.patent_id
WHERE contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
LIMIT 10
;

-- Steven P. Jobs を寄与者(contributor)とする特許を検索する。
SELECT
    patent_index.patent_id,
    invention_title,
    patent_type,
    application_date,
    contributor_name
FROM
    cybersyn_us_patent_grants.cybersyn.uspto_contributor_index AS contributor_index
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_contributor_relationships AS relationships
ON (contributor_index.contributor_id = relationships.contributor_id)
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index AS patent_index
ON relationships.patent_id = patent_index.patent_id
WHERE
    contributor_index.contributor_name ILIKE 'Steven P. Jobs'
;
-- タイトルに OLED を含む特許をすべて取得する。
SELECT
    patent_id,
    invention_title,
    patent_type,
    application_date
FROM
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index
WHERE
    invention_title ILIKE ANY ('%OLED%');

-- 2-1. SQL UDF作成
-- 以下ルールに基づいて、特許ごとに「TRUE(合格)/FALE(不合格)」を返却するSQL UDF（ユーザ定義関数）を作成する
--  　<ルール>
--     申請日(APPLICATION_DATE) と発行日(PUBLICATION_DATE)の差が、以下に該当する場合はTRUE、それ以外はFALSEを返却する。
--  　　Reissue（再発行）特許 → APPLICATION_DATE(申請) と PUBLICATION_DATE(発行日) の差が 365日以内
--  　　Design（意匠）特許    → APPLICATION_DATE と PUBLICATION_DATE の差が2年(730日)以内

--      ※今回は、うるう年(366日になるケース)は考慮しないこととした。

-- 参照マニュアル
--  　Scalar SQL UDFs：今回の課題のメイン。ユーザ定義関数をSQLで作成する場合のマニュアル
-- https://docs.snowflake.com/en/developer-guide/udf/sql/udf-sql-scalar-functions
--  　DATEDIFF：2つ日付の差分を計算
--     https://docs.snowflake.com/en/sql-reference/functions/datediff
--     →Snowflakeの場合、引数2 - 引数1の減算なので注意。他DWHの場合は、逆のケースあり。
--   LOWER：文字列の小文字変換
--         https://docs.snowflake.com/ja/sql-reference/functions/lower
--   TRIM：先頭、末尾の文字削除（デフォルトは単一空白文字
--        https://docs.snowflake.com/ja/sql-reference/functions/trim
CREATE OR REPLACE FUNCTION FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(
    application_date DATE,
    publication_date DATE,
    patent_type STRING
)
RETURNS BOOLEAN
COMMENT = 'アメリカ特許：有効期間判定（MEMOIZABLE(メモ化)なし）'
AS
$$
CASE
    WHEN application_date IS NULL OR publication_date IS NULL THEN FALSE
    WHEN publication_date < application_date THEN FALSE
    WHEN TRIM(LOWER(patent_type)) = 'reissue' AND DATEDIFF(DAY, application_date, publication_date) <= 365 THEN TRUE
    WHEN TRIM(LOWER(patent_type)) = 'design' AND DATEDIFF(DAY, application_date, publication_date) <= 730 THEN TRUE
    ELSE FALSE
END
$$;

-- 3-1.UDFを使ってみる/特許タイプ：Reissue
--  ※ NVIDIA CORPORATIONの特許では該当なしのようだったため、特許企業（団体?）を絞らず集計
SELECT patent_index.patent_id
    , invention_title
    , patent_type
    , application_date 
    , document_publication_date
    ,FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) AS validate_date_check -- 今回のUDFの実行結果(TRUE/FALSEを返却)
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
    relationships.contribution_type = 'Assignee - United States Company Or Corporation'
    AND patent_type = 'Reissue'
    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) -- TRUEの場合をチェックする用の条件
--    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) = FALSE -- FALSEの場合をチェックする用の条件

    LIMIT 10
;

-- 3-1.UDFを使ってみる/特許タイプ：Design
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

-- 応用：SQL UDFに関するオプションの利用（MEMORAIZE）

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

-- MEMOIZABLE有/無でクエリの実行時間の差が出るかの検証
-- TODO☆：検証用のクエリ、想定結果を記載














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