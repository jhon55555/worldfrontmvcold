using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Data
{
    public class SqlClientService : ISqlClientService
    {
        public DataSet Execute(string spName, string connectionString, IList<SqlParameter> sqlParameter)
        {
            using (var connaction = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = connaction;
                    command.CommandText = spName;
                    command.CommandType = CommandType.StoredProcedure;

                    if (sqlParameter.Any())
                    {
                        foreach (var item in sqlParameter)
                        {
                            command.Parameters.Add(item);
                        }
                    }
                    using (var da = new SqlDataAdapter())
                    {
                        da.SelectCommand = command;

                        var ds = new DataSet();

                        try
                        {
                            da.Fill(ds);
                        }
                        catch (System.Exception)
                        {
                        }
                        finally
                        {
                            connaction.Close();
                            connaction.Dispose();
                            command.Dispose();
                            da.Dispose();
                        }

                        return ds;
                    }
                }
            }
        }
        public DataSet ExecuteTOut(string spName, string connectionString, IList<SqlParameter> sqlParameter)
        {
            using (var connaction = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = connaction;
                    command.CommandText = spName;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = 0;

                    if (sqlParameter.Any())
                    {
                        foreach (var item in sqlParameter)
                        {
                            command.Parameters.Add(item);
                        }
                    }
                    using (var da = new SqlDataAdapter())
                    {
                        da.SelectCommand = command;

                        var ds = new DataSet();
                        try
                        {
                            da.Fill(ds);
                        }
                        catch (System.Exception)
                        {
                        }
                        finally
                        {
                            connaction.Close();
                            connaction.Dispose();
                            command.Dispose();
                            da.Dispose();
                        }

                        return ds;
                    }
                }
            }
        }
        public void ExecuteAsync(string spName, string connectionString, IList<SqlParameter> sqlParameter)
        {
            using (var connaction = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = connaction;
                    command.CommandText = spName;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = 45;
                    //connaction.ConnectionTimeout = 40000;
                    if (sqlParameter.Any())
                    {
                        foreach (var item in sqlParameter)
                        {
                            command.Parameters.Add(item);
                        }
                    }
                    connaction.Open();
                    command.ExecuteNonQuery();
                    connaction.Close();
                    connaction.Dispose();
                    command.Dispose();
                }
            }
        }
        public object ExecuteScaler(string spName, string connectionString, IList<SqlParameter> sqlParameter)
        {
            using (var connaction = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = connaction;
                    command.CommandText = spName;
                    command.CommandType = CommandType.StoredProcedure;

                    if (sqlParameter.Any())
                    {
                        foreach (var item in sqlParameter)
                        {
                            command.Parameters.Add(item);
                        }
                    }
                    connaction.Open();
                    var str = command.ExecuteScalar();
                    connaction.Close();
                    connaction.Dispose();
                    command.Dispose();
                    return str;
                }
            }
        }
    }
}
