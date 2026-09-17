using Microsoft.Data.SqlClient;

// En gemensam plats för anslutningssträngen, så att alla funktioner hämtar
// lösenordet på samma sätt: från app setting "SqlPassword" (Key Vault-referens).
public static class SqlConfig
{
    public static string ConnectionString
    {
        get
        {
            var password = Environment.GetEnvironmentVariable("SqlPassword");
            if (string.IsNullOrEmpty(password))
                throw new InvalidOperationException(
                    "App setting 'SqlPassword' saknas eller kunde inte läsas från Key Vault.");

            // Builder istället för strängformattering: tecken som ; eller = i lösenordet
            // förstör annars anslutningssträngen.
            return new SqlConnectionStringBuilder
            {
                DataSource = "sql-fabrikdata.database.windows.net",
                InitialCatalog = "db-fabrikdata",
                UserID = "sqladmin",
                Password = password,
                Encrypt = true
            }.ConnectionString;
        }
    }
}
