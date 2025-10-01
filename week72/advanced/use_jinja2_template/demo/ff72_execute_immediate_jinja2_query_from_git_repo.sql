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

-- リポジトリステージを更新する
-- リモートリポジトリに変更があり、その内容をリポジトリステージに反映するにはALTER GIT REPOSITORY コマンドを使用します。これにより Git リポジトリのコンテンツをリポジトリステージにフェッチできます。
ALTER GIT REPOSITORY frosty_friday_git_stage FETCH;
