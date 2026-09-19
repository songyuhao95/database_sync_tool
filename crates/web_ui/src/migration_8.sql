BEGIN IMMEDIATE;
ALTER TABLE instances ADD COLUMN database_names_json TEXT NOT NULL DEFAULT '[]';
UPDATE instances
SET database_names_json = CASE
    WHEN kind='postgresql' AND database_name<>''
    THEN '["' || replace(database_name,'"','') || '"]'
    ELSE '[]'
END;
PRAGMA user_version=8;
COMMIT;
