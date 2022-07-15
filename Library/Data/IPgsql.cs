using Npgsql;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace DimFrontOPenser.DataPg
{
    public interface IPgsql
    {
        DataSet Execute(string spName, string connectionString, IList<NpgsqlParameter> sqlParameter);
        DataSet ExecuteTOut(string spName, string connectionString, IList<NpgsqlParameter> sqlParameter);
        void ExecuteAsync(string spName, string connectionString, IList<NpgsqlParameter> sqlParameter);
        object ExecuteScaler(string spName, string connectionString, IList<NpgsqlParameter> sqlParameter);
    }
}
