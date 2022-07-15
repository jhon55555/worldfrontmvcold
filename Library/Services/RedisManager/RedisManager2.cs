using Common;
using Newtonsoft.Json;
using Services.RedisManager;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Services.RedisManage
{

    //public enum RedisServer
    //{
    //    Local = 1,
    //    Odds = 2
    //}

    public class RedisManager2 : IRedisManager2
    {
        private readonly RedisConnection2 _RedisCon;
        private readonly ConfigItems _config;
        public RedisManager2(RedisConnection2 RedisCon, ConfigItems config)
        {
            _RedisCon = RedisCon;
            _config = config;
        }
        #region Methods

        public T Get<T>(string key, int serverr = -1)
        {
            IDatabase db;
            db = RedisConnection2.ConnectionLocal.GetDatabase(5);
            Boolean dd7 = db.StringGet(key).IsNull;
            if (!dd7)
                return JsonConvert.DeserializeObject<T>(db.StringGet(key));
            else
                return JsonConvert.DeserializeObject<T>("");
        }
        public bool IsExist(string key, int serverr = -1)
        {
            IDatabase db;
            db = RedisConnection2.ConnectionLocal.GetDatabase(ConfigItems.RedisStaC);
            if (db.KeyExists(key))
                return true;
            else
                return false;
        }
        public IList<T> GetAllkeysData<T>(string pattern, int serverr = 1)
        {
            IDatabase db;
            IList<T> data = new List<T>();
            db = RedisConnection2.ConnectionLocal.GetDatabase(ConfigItems.RedisStaC);
            foreach (var item in pattern.TrimEnd(',').Split(','))
            {
                var keys = RedisConnection2.ServerLocal.Keys(ConfigItems.RedisStaC, pattern: item + "*").ToArray();
                foreach (var key1 in keys)
                //foreach (var key in pattern.TrimEnd(',').Split(','))
                {
                    if (key1.ToString().Contains('_'))
                    {
                        Boolean dd1 = db.StringGet(key1).IsNull;
                        if (!dd1)
                            data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(key1)));
                    }
                }
            }
            return data;
        }
        #endregion
    }
}
