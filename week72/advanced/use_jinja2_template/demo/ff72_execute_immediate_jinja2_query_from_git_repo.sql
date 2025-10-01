USE DATABASE FF72_DB;
USE SCHEMA SETUP;

-- 0. Git REPOSITORYの中身確認
LIST @frosty_friday_git_stage/branches/main;
-- 1. Gitリポジトリに格納されているSQLファイルの実行
--    Week72のチャレンジSnowSight上ではなく、Gitリポジトリ内のファイルを呼び出す形式で実行する。


-- (1)Week72のチャレンジを行う用のクエリファイルを指定のGitリポジトリから検索する。
LIST @frosty_friday_git_stage/branches/main PATTERN='.*jinja.*\\.sql';

-- (2)実際に実行する前に、DRY_RUNオプションで処理内容を確認する。
EXECUTE IMMEDIATE FROM '@frosty_friday_git_stage/branches/main/week72/advanced/use_jinja2_template/demo/ff72_with_jinja_template.sql' 
USING (database_name=>'frosty_friday', schema=>'basic_72')
DRY_RUN=TRUE;

-- (3)実際に、Gitリポジトリ上のSQLファイルを呼び出し実行する。
EXECUTE IMMEDIATE FROM '@frosty_friday_git_stage/branches/main/week72/advanced/use_jinja2_template/demo/ff72_with_jinja_template.sql' 
USING (database_name=>'frosty_friday', schema=>'basic_72')
;

-- (3)内で実行したINSERTの結果が、反映先のデータベース、スキーマ以外一致することを確認する。
WITH note_use_template_exe AS (
    -- テンプレート利用なし
    SELECT
        'なし' AS use_template_category ,
        *
    FROM
        ff72_db.solutions.week72_employees
)
,use_template_exe AS (
    -- テンプレート利用有
    SELECT
        'あり' AS use_template_category ,
        *
    FROM
        frosty_friday.basic_72.week72_employees
)
SELECT
    *
FROM
    note_use_template_exe nut
LEFT JOIN
    use_template_exe ut
ON nut.employeeid = ut.employeeid
;

-- リポジトリステージを更新する(参考)
-- リモートリポジトリに変更があり、その内容をリポジトリステージに反映するにはALTER GIT REPOSITORY コマンドを使用します。これにより Git リポジトリのコンテンツをリポジトリステージにフェッチできます。
ALTER GIT REPOSITORY frosty_friday_git_stage FETCH;
