-- 1. create database for this challenge 
CREATE DATABASE IF NOT EXISTS CHALLENGE_DB;

-- 2. Create the schema 
CREATE SCHEMA IF NOT EXISTS CHALLENGE_DB.INGESTION; -- as bronze layer
CREATE SCHEMA IF NOT EXISTS CHALLENGE_DB.SILVER; -- as trnaformation layer
CREATE SCHEMA IF NOT EXISTS CHALLENGE_DB.GOLD; -- as analytic layer

-- 3. Seleccionar el contexto (Esto es lo que te faltaba)
USE DATABASE CHALLENGE_DB;
USE SCHEMA INGESTION;
USE ROLE ACCOUNTADMIN; -- 