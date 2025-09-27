-- frosty friday Week 72 BASIC
--  EXECUTE IMMEDIATE FROM.

-- 【チャレンジ内容】
--    Snowflakeの新機能「EXECUTE IMMEDIATE FROM」を使用しするチャレンジ。

--   外部ステージ（s3://frostyfridaychallenges）にあるSQLコマンド(INSERT)を実行し、
--   指定のテーブル「week72_employees」にSQLコマンドの実行結果を反映する必要がある。

--   ref：https://docs.snowflake.com/sql-reference/sql/execute-immediate-from

--   「EXECUTE IMMEDIATE FROM」コマンドとは？
--      ・FROM句に指定したパスのファイルを実行することが可能なコマンド。
--      ・ステージ、またはGitリポジトリに格納されているファイルである必要がある。
--      ・Jinja2テンプレートが利用でき、USING句でパラメータ指定することができる（2025/09時点プレビュー）

-- 0. 事前準備
--    Frosty Friday 72用のDATABASE、SCHEMAの作成
CREATE OR REPLACE DATABASE FF72_DB;
CREATE OR REPLACE SCHEMA FF72_DB.PUBLIC;

USE DATABASE FF72_DB;
USE SCHEMA PUBLIC;

-- Week 72 Challenge
-- 1. テーブル作成
create table week72_employees (
    employeeid int,
    firstname string,
    lastname string,
    dateofbirth date,
    position string
);

-- 2. ステージ作成・内容確認
-- Week 72で提示されているSQLファイルが格納されている場所「s3://frostyfridaychallenges」を参照するステージを作成
--  ref：https://docs.snowflake.com/ja/sql-reference/sql/create-stage
CREATE OR REPLACE STAGE ff72_challenges URL='s3://frostyfridaychallenges';

-- (1) 指定されたURL内にどんなファイルが格納されているか確認
--      → 今回使うSQLファイル含め、frosty fridaynに使用すると思われるファイルが格納されている。
--         → Week 72用のSQLファイルを探す必要がある。
LIST @ff72_challenges;

-- (2) Week 72に関係するファイルに絞りこんで確認
--     ref：https://docs.snowflake.com/ja/sql-reference/sql/list
--　　　　→ オプションのパラメーター／PATTERN = 'regex_pattern'：正規表現を指定しファイルを絞り込むことができる。
--  　実行してみる
--      →s3://frostyfridaychallenges/challenge_72/insert.sqlというファイル名が表示される。
--   　  →ファイル名がchallenge_72で番号が72で一致なのであっていそう。
--        ただファイルの中身を見ないと判断できない部分があるので、実際にファイルのクエリを実行する前にファイルの中身を確認してみる。
LIST @ff72_challenges PATTERN='.*72.*\\.sql';

-- 3.指定ファイルのクエリ実行（今回のチャレンジ内容）

--   (1) 実行クエリ確認
--       実際に実行する前に、DRY_RUNオプションを利用しどんなクエリが実行されるか確認する。

--      【コマンドを実行すると表示されるクエリ】
--        INSERT INTO week72_employees (EmployeeID, FirstName, LastName, DateOfBirth, Position) 
--        VALUES 
--        (1, 'John', 'Doe', '1985-07-24', 'Software Engineer'),
--        (2, 'Jane', 'Smith', '1990-04-12', 'Project Manager'),
--        (3, 'Emily', 'Jones', '1992-11-08', 'Graphic Designer'),
--        (4, 'Michael', 'Brown', '1988-01-15', 'System Administra
EXECUTE IMMEDIATE FROM '@ff72_challenges/challenge_72/insert.sql' DRY_RUN=TRUE;

-- (2)クエリ実行
--    危険そうなクエリではない、かつ、今回テーブルに反映されるはずのデータと一致していそうなので、DRY_RUNオプションを外して実際に実行してみる。
--     → 4件データがINSERTされたことが確認できる。(1)のクエリでINSERTされる件数と一致していそう！
EXECUTE IMMEDIATE FROM '@ff72_challenges/challenge_72/insert.sql';

-- (3)クエリ実行後の確認
--    今回のチャレンジで期待しているレコード（チャレンジページの下部の表）の通りにレコードが反映されているか確認する。
--      →確認した結果問題なさそう。
SELECT * FROM week72_employees ORDER BY employeeid;
-- EOF
