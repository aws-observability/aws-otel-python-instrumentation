import sqlparse
import json
from typing import Dict, Set

DIALECT_KEYWORDS: Dict[str, Set[str]] = {
    "CUSTOM_DIALECT": {
        "DROP VIEW",
        "INNER JOIN",
        "SELECT TOP",
        "NOT NULL",
        "TRUNCATE TABLE",
        "OUTER JOIN",
        "CREATE TABLE",
        "ALTER TABLE",
        "CREATE INDEX",
        "ORDER BY",
        "BACKUP DATABASE",
        "INSERT INTO SELECT",
        "UNION ALL",
        "PRIMARY KEY",
        "IS NOT NULL",
        "DROP CONSTRAINT",
        "TOP",
        "IS NULL",
        "LEFT JOIN",
        "CREATE DATABASE",
        "DROP DATABASE",
        "FOREIGN KEY",
        "DROP DEFAULT",
        "SELECT INTO",
        "GROUP BY",
        "SELECT DISTINCT",
        "DROP COLUMN",
        "RIGHT JOIN",
        "DROP INDEX",
        "CREATE VIEW",
        "ALTER COLUMN",
        "INSERT INTO",
        "DROP TABLE",
        "FULL OUTER JOIN",
    },
    "ANSI_SQL": sqlparse.keywords.KEYWORDS_COMMON,
    "PL_PGSQL": sqlparse.keywords.KEYWORDS_ORACLE,
    "POSTGRESQL": sqlparse.keywords.KEYWORDS_PLPGSQL,
    "HQL": sqlparse.keywords.KEYWORDS_HQL,
    "KEYWORDS_MSACCESS": sqlparse.keywords.KEYWORDS_MSACCESS,
    "KEYWORDS": sqlparse.keywords.KEYWORDS,
}

keywords = set()
keyword_list = list(keywords)

for keyword in DIALECT_KEYWORDS.values():
    keywords.update(keyword)

with open("keywords.json", "w") as json_file:
    json.dump(keyword_list, json_file)
