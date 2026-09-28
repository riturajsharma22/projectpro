-- A database-only user for the Logic App (no server-level login needed)
CREATE USER logicapp_reader WITH PASSWORD = '<STRONG_PASSWORD>';

-- Read-only access to all tables in this database
ALTER ROLE db_datareader ADD MEMBER logicapp_reader;

SELECT dp.name AS user_name, r.name AS role_name
FROM sys.database_role_members rm
JOIN sys.database_principals r  ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
WHERE dp.name = 'logicapp_reader';