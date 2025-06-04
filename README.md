# How can I use a service principal to read and write to a database from Databricks?

This demo will show how you can use an Entra service principal to read and write to an Azure SQL DB or Azure Postgres database from a Databricks notebook.

## Solution
- [Prerequisites](#prerequisites)
- [Azure SQL DB](#azure-sql-db)
- [Azure Postgres](#azure-postgres)

## Prerequisites

### Create an Entra app registration
First, navigate to your tenant's Entra Id directory and add a new [app registration](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).  This will create an Entra Id service principal that you can use to authenticate to Azure resources:
![app-registration](./databricks/AppRegistration.png)

Next, create a secret for the service principal. 

Note: You'll need Tenant ID, service principal name, client ID and secret for the steps below.
![add-secret1](./databricks/AddSecret1.png)
![add-secret2](./databricks/AddSecret2.png)

The secret value will appear on the screen. Do not leave the page before recording it ([it will never be displayed again](https://learn.microsoft.com/en-us/entra/identity-platform/how-to-add-credentials?tabs=client-secret#add-a-credential-to-your-application)). You may want to keep the browser tab open and complete the remaining steps in a second tab for convenience.  
![secret-id-value](./databricks/SpSecretIDValue.png)

### Create an Azure Key Vault
Create an Azure Key Vault in the same region
![azure-keyvault](./databricks/CreateKeyVault.png)

Assign yourself and the Azure Databricks application the Key Vault Secrets Officer role. (Databricks will access the key vault referenced in your secrets scope using the Databricks application's own service principal, which is unique to your tenant. You might expect a Unity Catalog-enabled workspace to use the workspace's managed identity to connect to the key vault, but unfortunately that's not the case.)
![add-role](./databricks/AddKeyVaultRole.png)
![secrets-officer](./databricks/KVSecretsOfficerRole.png)
![azure-databricks](./databricks/AssignDatabricksAppRBAC.png)

Add three secrets to the key vault: one for your tenant id, another for the service principal client Id (not the secret id) and a third for the secret value.  You will need these to authenticate as the service principal in your Databricks notebook. Refer back to the secret value in the other browser tab as needed.
![secret-id](./databricks/CreateSPId.png)
![secret-value](./databricks/CreateSPSecret.png)



### Create a Databricks workspace
Many of the new Databricks features like serverless, data lineage and managed identity support require Unity Catalog. Unity Catalog is normally enabled now by default when you create a new workspace and gives you the most authentication options when connecting to Azure resources like databases. The steps described in this demo should work for both Unity Catalog-enabled workspaces and workspaces using the legacy hive metastore.
![create-workspace](./databricks/create-databricks-workspace.png)

### Create a Databricks secret scope
Next, we need to [create a secrets scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/) for our Databricks workspace.  Open your workspace and add ```#secrets/createScope``` after the databricks instance url in your browser. It's [case sensitive](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/#create-an-azure-key-vault-backed-secret-scope-1) so be sure to use a capital S in 'createScope':
![dbx-scope](./databricks/CreateDBXSecretsScope.png)

A page to configure a new secrets scope should appear.  Give your secrets scope a name and paste the key vault uri (DNS Name) and resource id into the respective fields below.  You can find these on the properties blade of your key vault.
![dbx-uri-resource-id](./databricks/createDBXSecretsScope2.png)
![kv-properties](./databricks/KeyVaultProperties.png)

## Create a Databricks cluster
If you just want to read from a database with a service principal, you can use the serverless cluster.  Just add the azure-identity library to the environment configuration.

![azure-identity](./databricks/AddServerlessClusterLibrary.png)

If you also want to *write* to the database, however, you'll need to use a provisioned cluster.
![create-cluster](./databricks/CreateCluster.png)

After the cluster has been created, click on the cluster name in the list and then switch to the libraries tab to install azure-identity with PyPi
![install-library](./databricks/InstallLibrary.png)

## Azure SQL DB
### Create Database
Create an Azure SQL database with Entra Id authentication. You can use the Adventure Works LT sample to pre-populate it with data if you like. For the purposes of this demo, we will only be working with the information_schema.
![adventure-works](./databricks/AdventureWorksLTDB.png)

Then connect to the database using the Entra Id admin user and [create a user for the service principal](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-service-principal-tutorial?view=azuresql#create-the-service-principal-user). You can use the Query editor in the portal or SQL Server Management Studio.
![query-editor](./databricks/QueryEditor.png)
![create-dbuser](./databricks/CreateDBUser.png)

``` SQL
CREATE USER [sql-dbx-read-write-test-sp] FROM EXTERNAL PROVIDER;
```

Add the dbmanagedidentity user to database roles db_datareader, db_datawriter and db_ddladmin. This will allow the managed identity to read and write data to existing tables. Using overwrite mode will automatically drop and recreate a table before writing to it.
``` SQL
ALTER ROLE db_datareader
ADD MEMBER [sql-dbx-read-write-test-sp];

ALTER ROLE db_datawriter
ADD MEMBER [sql-dbx-read-write-test-sp];

ALTER ROLE db_ddladmin
ADD MEMBER [sql-dbx-read-write-test-sp];
```



## Create a Databricks notebook
Paste the code below into the first cell of the notebook. Using the dbutils library, retrieve the tenant id, client id and service principal secret from the key vault referenced in the secrets scope and get an Entra token.
``` python
from azure.identity import ClientSecretCredential

tenant_id = dbutils.secrets.get(scope = "default", key = "tenant-id")
client_id = dbutils.secrets.get(scope = "default", key = "sql-dbx-read-write-test-sp-client-id")
client_secret = dbutils.secrets.get(scope = "default", key = "sql-dbx-read-write-test-sp-secret")

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
token = credential.get_token("https://database.windows.net/.default").token
```

In the next cell, create a jdbc connection to the database and query a list of tables from the information schema.
``` python
jdbc_url = "jdbc:sqlserver://sql-xxxx.database.windows.net:1433;database=testdb"

connection_properties = {
    "accessToken": token,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read from a table
df = spark.read.jdbc(url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties)
df.show()
```
The output should look something like this
![info-schema](./databricks/ReadInfoSchema.png)

If you're using a provisioned cluster, you can run the code below to write to the database.
``` python
df.write.jdbc(
    url=jdbc_url,
    table="dbo.Test",
    mode="overwrite",
    properties=connection_properties
)
```

To verify that the data was written as expected, you can read the table data into a dataframe
``` python
df_check = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.Test",
    properties=connection_properties
)
df_check.show()
```

You can find the complete notebook [here](./databricks/Read_Write_SQL_DB_SP.ipynb)

## Azure Postgres
### Create Database
First [create an Azure Database for PostgreSQL flexible server with Entra Id authentication](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-configure-sign-in-azure-ad-authentication). Then connect to the **postgres** database with the Entra ID administrator user. 

In the example below, we connect using bash commands in [Azure Cloud Shell](https://learn.microsoft.com/en-us/azure/cloud-shell/get-started/classic?tabs=azurecli) in the portal. (You can use the [PostgreSQL add-in in VS Code](https://marketplace.visualstudio.com/items?itemName=ms-ossdata.vscode-pgsql) instead if you prefer.)
![create-PGUser](./databricks/connect-postgres3.png)

Set the environment variables in the Cloud Shell window.
``` bash
export PGHOST=psql-xxxxx.postgres.database.azure.com
export PGUSER=user@domain.com
export PGPORT=5432
export PGDATABASE=postgres
export PGPASSWORD="$(az account get-access-token --resource https://ossrdbms-aad.database.windows.net --query accessToken --output tsv)" 
```

Now simply type psql. The environment variables set above will be used automatically to connect.
``` bash
psql
```


Once connected, run the statement below to create a user for the service principal
``` python
SELECT * FROM pgaadauth_create_principal('sql-dbx-read-write-test-sp',false, false);
```
![create-dbuser](./databricks/CreatePGUser.png)

Quit your connection to the postgres database and then open a new connection to your **application** database (```testdb``` in our example). Grant the service principal user read and write privileges on all tables in the public schema
![connect-application-db](./databricks/connect-testdb2.png)

```SQL
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "sql-dbx-read-write-test-sp";

GRANT ALL PRIVILEGES ON SCHEMA public TO "sql-dbx-read-write-test-sp";
```
![grant-pguser](./databricks/GrantPGUser.png)

### Create a Databricks notebook
Paste the code below into the first cell of the notebook. Using the dbutils library, retrieve the tenant id, client id and service principal secret from the key vault referenced in the secrets scope and get an Entra token. The url used to get the token is different from the one we used above to authenticate to [Azure SQL DB](#azure-sql-db).
``` python
from azure.identity import ClientSecretCredential

sp_name = "sql-dbx-read-write-test-sp"
tenant_id = dbutils.secrets.get(scope = "default", key = "tenant-id")
client_id = dbutils.secrets.get(scope = "default", key = "sql-dbx-read-write-test-sp-client-id")
client_secret = dbutils.secrets.get(scope = "default", key = "sql-dbx-read-write-test-sp-secret")

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default").token
```

In the next cell, add the following code to connect to and read data from the database into a dataframe. You need to specify the service principal name as the user and pass the token for the password. Once the connection is made, however, we read and write to the database the same way we did for SQL.
``` python
jdbc_url = "jdbc:postgresql://psql-xxxxxxx.postgres.database.azure.com:5432/testdb"

connection_properties = {
    "user": sp_name,
    "password": token,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslfactory": "org.postgresql.ssl.NonValidatingFactory"
}

# Read from a table
df = spark.read.jdbc(url=jdbc_url, table="information_schema.tables", properties=connection_properties)
display(df)
```

Add a new cell and paste in the code below to write the contents of the dataframe to a new table in the database. The only difference to the SQL code above is that we are using the public schema in place of dbo.
``` python
df.write.jdbc(
    url=jdbc_url,
    table="public.Test",
    mode="overwrite",
    properties=connection_properties
)
```

Finally, let's add a cell to read the table contents into a dataframe to confirm that the data was written.
``` python
df_check = spark.read.jdbc(
    url=jdbc_url,
    table="public.Test",
    properties=connection_properties
)
df_check.show()
```

You can download the complete notebook [here](./databricks/Read_Write_PSQL_DB_SP.ipynb)