using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Services.RedisManage
{
    public interface IRedisManagerOther
    {

        Task<T> Getasync<T>(string key, int serverr = -1);
        Task<IList<T>> GetAllkeysData<T>(string pattern, int serverr = 1);
        Task<bool> IsExistasync(string key, int serverr = -1);
        T Get<T>(string key, int serverr = -1);
        bool IsExist(string key, int serverr = -1);
        IDatabaseAsync getDb(int serverr = 1);
    }
}
