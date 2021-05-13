CREATE DATABASE IF NOT EXISTS nested_column;
use nested_column;
CREATE TABLE IF NOT EXISTS parquet(a struct<a1:string,a2:string,a3:string,a4:bigint,a5:array<string>,a6:string,a7:string,a8:string,a9:boolean,a10:string,a11:string,a12:string,a13:string,a14:bigint,a15:string,a16:string,a17:array<string>,a18:bigint,a19:string,a20:array<string>,a21:string,a22:array<string>,a23:string,a24:bigint,a25:bigint,a26:string,a27:string,a28:bigint,a29:string,a30:bigint>) stored as parquet;

