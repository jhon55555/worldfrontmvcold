using Common;
using Newtonsoft.Json;
using Services.RedisManager;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Services.RedisManage
{

    //public enum RedisServer
    //{
    //    Local = 1,
    //    Odds = 2
    //}

    public class RedisManagerCric : IRedisManagerCric
    {
        private readonly RedisConnectionCric _RedisCon;
        private readonly ConfigItems _config;
        public RedisManagerCric(RedisConnectionCric RedisCon, ConfigItems config)
        {
            _RedisCon = RedisCon;
            _config = config;
        }
        #region Methods

        public T Get<T>(string key, int serverr = -1)
        {
            IDatabase db;
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            Boolean dd7 = db.StringGet(key).IsNull;
            if (!dd7)
            {
                var data = db.StringGet(key);
                return JsonConvert.DeserializeObject<T>(data);
            }
            return JsonConvert.DeserializeObject<T>("");
        }
        public async Task<T> Getasync<T>(string key, int serverr = -1)
        {
            IDatabaseAsync db;
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            Boolean dd7 = db.StringGetAsync(key).Result.IsNull;
            if (!dd7)
            {
                var data = await db.StringGetAsync(key);
                return JsonConvert.DeserializeObject<T>(data);
            }
            return JsonConvert.DeserializeObject<T>("");
        }
        public async Task<bool> IsExistasync(string key, int serverr = -1)
        {
            IDatabaseAsync db;
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            if (await db.KeyExistsAsync(key))
                return true;
            else
                return false;
        }
        public static async Task<IEnumerable<string>> GetSetMembers(string key, int serverr = -1)
        {
            IDatabaseAsync db;
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            //int d = Convert.ToInt32(s[1]) + 1;
            key = "Set_" + key;
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            var members = await db.SetMembersAsync(key);
            return members.Select(x => x.ToString());
        }
        public async Task<IList<T>> GetAllkeysData<T>(string pattern, int serverr = 1)
        {
            IDatabaseAsync db;
            IList<T> data = new List<T>();
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            var keys = await RedisManagerCric.GetSetMembers(pattern, serverr);
            foreach (var key1 in keys)
            {
                Boolean dd1 = db.StringGetAsync(key1).Result.IsNull;
                if (!dd1)
                    data.Add(JsonConvert.DeserializeObject<T>(db.StringGetAsync(key1).Result));
            }
            return data;
        }
        public bool IsExist(string key, int serverr = -1)
        {
            IDatabase db;
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            if (db.KeyExists(key))
                return true;
            else
                return false;
        }
        public IList<T> GetAllkeys<T>(string pattern, int serverr = 1)
        {
            IDatabaseAsync db;
            IList<T> data = new List<T>();
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            ConfigItems.Redd = s[0];
            db = RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
            var keys =  RedisManagerCric.GetSetMembers(pattern, serverr);
            //foreach (var key1 in keys.Result)
            //{
            //    Boolean dd1 = db.StringGetAsync(key1).Result.IsNull;
            //    if (!dd1)
            //        data.Add(JsonConvert.DeserializeObject<T>(db.StringGetAsync(key1).Result));
            //}
            return data;
        }
        public IDatabaseAsync getDb(int serverr = 1)
        {
            var s = ConfigItems.RedisServerCric[serverr].Split('|');
            return RedisConnectionCric.ConnectionLocal.GetDatabase(Convert.ToInt32(s[1]));
        }

        #endregion
    }
}
