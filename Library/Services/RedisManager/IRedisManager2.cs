using System;
using System.Collections.Generic;

namespace Services.RedisManage
{
    public interface IRedisManager2
    {

        //void SetInMinDyn<T>(string key, int expireTime, T cacheItem, int serverr = 1);

        T Get<T>(string key, int serverr = -1);
        //T GetDyn<T>(string key, int serverr = 1);
        //T AddOrGetExisting<T>(string key, int expireTime, Func<T> valueFactory, int serverr = 1);
        bool IsExist(string key, int serverr = -1);
        IList<T> GetAllkeysData<T>(string pattern, int serverr = 1);
      
    }
}
