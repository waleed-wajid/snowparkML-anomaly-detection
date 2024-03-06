/* 
Setup database and schemas for project
*/

--create role for anomaly detection task
use role SYSADMIN;

-- create database for project
create or replace database DEV_ML_WALEED;
use database DEV_ML_WALEED;

create or replace schema RAW;
create schema DEV_ML_WALEED.PROD;
use schema RAW;
use warehouse DEV;

-- stage to store ML assets like preprocessing pipelines
create stage ML_ASSETS;