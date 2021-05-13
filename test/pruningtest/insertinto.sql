CREATE DATABASE IF NOT EXISTS nested_column;
use nested_column;
insert into table parquet select * from textfile;
