using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Services.RedisManager
{

    public enum RedisServer
    {
    }

    public class RedisManager : IRedisManager
    {
        #region Methods
        public async Task SetInMinasync<T>(string key, int expireTime, T cacheItem, int database = 0)
        {
            IDatabaseAsync db;
            db = RedisConnection.ConnectionLocal.GetDatabase(database);
            await db.StringSetAsync(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
            return;
        }
        public void SetInMin<T>(string key, int expireTime, T cacheItem, int database = 0)
        {
            IDatabase db = RedisConnection.ConnectionLocal.GetDatabase(database);
            db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
        }
        public T Get<T>(string key, int database = 0)
        {
            IDatabase db = RedisConnection.ConnectionLocal.GetDatabase(database);
            return JsonConvert.DeserializeObject<T>(db.StringGet(key));
        }
        public bool IsExist(string key, int database = 0)
        {
            IDatabase db = RedisConnection.ConnectionLocal.GetDatabase(database);
            if (db.KeyExists(key))
                return true;
            else
                return false;
        }

        public void Remove(string key, int database = 0)
        {
            IDatabase db = RedisConnection.ConnectionLocal.GetDatabase(database);
            db.KeyDelete(key);
        }


        #endregion
    }
}
