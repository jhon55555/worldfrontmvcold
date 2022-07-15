using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Services.RedisManager
{
    public interface IRedisManager
    {
        Task SetInMinasync<T>(string key, int expireTime, T cacheItem, int database = 0);
        void SetInMin<T>(string key, int expireTime, T cacheItem, int database = 0);
        T Get<T>(string key, int database = 0);
        bool IsExist(string key, int database = 0);
        void Remove(string key, int database = 0);
    }
}
