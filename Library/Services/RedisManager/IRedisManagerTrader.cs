using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Services.RedisManager
{
    public interface IRedisManagerTrader
    {
        void Set<T>(string key, int expireTime, T cacheItem, int database = 0);

        void SetInMin<T>(string key, int expireTime, T cacheItem, int database = 0);

        void Set<T>(string key, TimeSpan expireTime, T cacheItem, int database = 0);

        T Get<T>(string key, int database = 0);

        T AddOrGetExisting<T>(string key, int expireTime, Func<T> valueFactory, int database = 0);

        bool IsExist(string key, int database = 0);

        void Remove(string key, int database = 0);

        void RemoveByPatterns(string pattern = "", int database = 0);

        IList<T> GetAllkeysData<T>(string pattern, int database = 0);

        IList<string> GetAllkeys(string pattern, int database = 0);
        Task<string> Getasyncstr(string key, int database = 1);
    }
}
