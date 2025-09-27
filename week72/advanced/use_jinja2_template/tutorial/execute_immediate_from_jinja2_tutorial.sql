EXECUTE IMMEDIATE FROM @frosty_friday_git_stage/scripts/setup.sql
    USING (env=>'dev', retention_time=>0);
