using System;
using System.Collections.Generic;

namespace Services.RedisManage
{
    public interface IRedisManager1
    {
        void Set<T>(string key, int expireTime, T cacheItem, int serverr = 1);

        void SetInMin<T>(string key, int expireTime, T cacheItem, int serverr = 1);
        //void SetInMinDyn<T>(string key, int expireTime, T cacheItem, int serverr = 1);

        void Set<T>(string key, TimeSpan expireTime, T cacheItem, int serverr = 1);

        T Get<T>(string key, int serverr = -1);
        //T GetDyn<T>(string key, int serverr = 1);
        //T AddOrGetExisting<T>(string key, int expireTime, Func<T> valueFactory, int serverr = 1);

        bool IsExist(string key, int serverr = -1);
        //bool IsExistDyn(string key, int serverr = 1);
        void Remove(string key, int serverr = 1);
        //void RemoveDyn(string key, int serverr = 1);
        void RemoveByPatterns(string pattern = "", int serverr = 1);

        IList<T> GetAllkeysData<T>(string pattern, int serverr = 1);
        void Delete(string pattern, int serverr = 1);
        //IList<T> GetAllkeysDataDyn<T>(string pattern, int serverr = 1);
        IList<T> GetSingalkeyData<T>(string pattern, int serverr = 1);

        IList<string> GetAllkeys(string pattern, int serverr = 1);
        void Flush(int serverr = 1);
    }
}
