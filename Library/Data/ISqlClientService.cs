using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Data
{
    public interface ISqlClientService
    {
        DataSet Execute(string spName, string connectionString, IList<SqlParameter> sqlParameter);
        DataSet ExecuteTOut(string spName, string connectionString, IList<SqlParameter> sqlParameter);
        void ExecuteAsync(string spName, string connectionString, IList<SqlParameter> sqlParameter);
        object ExecuteScaler(string spName, string connectionString, IList<SqlParameter> sqlParameter);
    }
}
