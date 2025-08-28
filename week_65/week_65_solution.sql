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
--   　3．以下のルールに基づいて、特許に「TRUE(有効）/FALSE(有効ではない)」を返す関数を作る：
--  　    <ルール>
--          出願日(APPLICATION_DATE) と公開日(PUBLICATION_DATE)の差が、以下に該当する場合はTRUE、それ以外はFALSEを返却する。
--  　　        Reissue（再発行）特許 → APPLICATION_DATE(出願日) と PUBLICATION_DATE(公開日) の差が 365日以内
--  　　        Design（意匠）特許    → APPLICATION_DATE(出願日) と PUBLICATION_DATE(公開日) の差が2年(730日)以内

-- → 色々な言語でUDFを作成でき、チャレンジ上の指定はないが、今回は「スカラSQL UDF(入力行ごとに1)」を作成、使ってみる流れとした。


-- 0.事前準備：各種設定、DB、スキーマ作成
USE ROLE SYSADMIN;
USE warehouse COMPUTE_WH_M; 

-- ForstyFriday用のDATABASE、SCHEMAを作成
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
FROM cybersyn_us_patent_grants.cybersyn.uspto_contributor_index AS contributor_index
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_contributor_relationships AS relationships
    ON contributor_index.contributor_id = relationships.contributor_id
INNER JOIN
    cybersyn_us_patent_grants.cybersyn.uspto_patent_index AS patent_index
    ON relationships.patent_id = patent_index.patent_id
WHERE contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
ORDER BY APPLICATION_DATE DESC
LIMIT 10
;
-- 2-1. スカラSQL UDF作成
-- 以下ルールに基づいて、特許ごとに「TRUE(有効)/FALE(有効ではない)」を返却するSQL UDF（ユーザ定義関数）を作成する
--  　<ルール>
--     出願日(APPLICATION_DATE) と公開日(PUBLICATION_DATE)の差が、以下に該当する場合はTRUE、それ以外はFALSEを返却する。
--  　　　Reissue（再発行）特許 → APPLICATION_DATE(出願日) と PUBLICATION_DATE(公開日) の差が 365日以内
--  　　  Design（意匠）特許    → APPLICATION_DATE(出願日) と PUBLICATION_DATE(公開日) の差が2年(730日)以内

--      ※スカラSQL UDF(UDFs):1回の呼び出しにつき、1行(レコード)を返却する
--       （【比較】表形式 SQL UDF(UDTFs)：1回の呼び出しにつき、テーブルを返却する。）
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
CREATE OR REPLACE FUNCTION FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.VALIDATE_PATENT_GAP_NO_MEMOIZABLE(
    application_date DATE,
    publication_date DATE,
    patent_type STRING
)
RETURNS BOOLEAN
COMMENT = 'アメリカ特許：有効期間判定'
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

-- 作成したUDFが反映されていることの確認
-- → データベースエクスプローラーで、対象UDFを検索して確認する。

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
    contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION' -- NVIDIA CORPORATIONで絞ると対象0県なのでコメントアウトする。
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
    AND patent_type = 'Reissue'
    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) -- TRUEの場合をチェックする用の条件
    --AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) = FALSE -- FALSEの場合をチェックする用の条件
    LIMIT 10
;


-- 3-1.UDFを使ってみる/特許タイプ：Design
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
    contributor_index.contributor_name ILIKE 'NVIDIA CORPORATION'
    AND relationships.contribution_type = 'Assignee - United States Company Or Corporation'
    AND patent_type = 'Design'
    AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) -- TRUEの場合をチェックする用の条件
    --AND FROSTY_FRIDAY_DB.WEEK_65_SCHEMA.validate_patent_gap_no_memoizable(application_date, document_publication_date, patent_type) = FALSE -- FALSEの場合をチェックする用の条件
    ORDER BY application_date desc
LIMIT 3
;

------------------
/*
SELECT
    QUERY_ID,
    QUERY_TYPE,
    CONVERT_TIMEZONE('Asia/Tokyo',START_TIME) AS START_TIME,
    CONVERT_TIMEZONE('Asia/Tokyo',END_TIME) AS END_TIME,
    (TOTAL_ELAPSED_TIME/ 1000.0) AS TOTAL_ELAPSED_TIME, -- 経過時間（ミリ秒 → 秒に変換)
FROM
    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10))
WHERE
    QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY START_TIME DESC
;
*/
/*
SELECT
    query_id,
    start_time,
    total_elapsed_time,
    child_queries_wait_time,
    query_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    AND QUERY_TYPE = 'SELECT' -- セッション設定(SESSION SET)のクエリは除外
    AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%' -- 実行時間計測のクエリ自体を除外
ORDER BY start_time DESC
;
*/
