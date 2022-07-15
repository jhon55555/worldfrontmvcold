using Newtonsoft.Json;
using Services.RedisManager;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Services.CacheManager
{

    public enum RedisServer
    {
        Local = 1,
        Casino = 2,
        Trader = 3,
        BeepPanel = 4
    }

    public class Redis1 : IRedis1
    {
        private readonly RedisCon RedisCon;
        public Redis1(RedisCon _RedisCon)
        {
            RedisCon = _RedisCon;
        }
        #region Methods
        //public IList<T> GetAllkeysData<T>(string pattern, int database = 1, int serverr = 1)
        //{
        //    IDatabase db;
        //    IList<T> data = new List<T>();

        //    if ((byte)RedisServer.Trader == serverr)
        //        db = RedisCon.ConnTrader.GetDatabase(database);
        //    else
        //        db = RedisCon.ConnLocal.GetDatabase(database);
        //    if (!string.IsNullOrEmpty(pattern))
        //    {
        //        var keys = RedisCon.ServerTrader.Keys(database, pattern: pattern + "*").ToArray();
        //        if (keys != null && keys.Count() > 0)
        //        {
        //            foreach (var item in keys)
        //            {
        //                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(item.ToString())));
        //            }
        //        }

        //    }
        //    return data;
        //}
        //public void Flush(string db)
        //{
        //    if (!string.IsNullOrEmpty(db))
        //    {
        //        foreach (var item in db.Split(','))
        //        {
        //            RedisCon.ServerLocal.FlushDatabase(Convert.ToInt32(item));
        //        }
        //    }

        //}

        public async Task<IEnumerable<T>> GetSetMembers<T>(string key, int database, int re)
        {
            IDatabaseAsync db;
            switch (re)
            {
                case 0:
                    {
                        db = RedisCon.ConnTrader.GetDatabase(database);
                        var members = await db.SetMembersAsync(key);
                        return await db.MGetAsync<T>(members.Select(x => x.ToString()));
                    }
                    //case 1:
                    //    {
                    //        db = RedisCon.ConnCommonfr.GetDatabase(database);
                    //        var members = await db.SetMembersAsync(key);
                    //        return await db.MGetAsync<T>(members.Select(x => x.ToString()));
                    //    }
                    //case 2:
                    //    {
                    //        db = RedisCon.ConnCommonfrnew.GetDatabase(database);
                    //        var members = await db.SetMembersAsync(key);
                    //        return await db.MGetAsync<T>(members.Select(x => x.ToString()));
                    //    }
                    //case 3:
                    //    {
                    //        _cacheManagerOther.UpdateSetMember(t1.FirstOrDefault().gmid.ToString(), "", redis);
                    //        break;
                    //    }
            }
            return null;
        }
        public async Task<string> GetSetMembersstr(string key, int database, int re)
        {
            IDatabaseAsync db;
            switch (re)
            {
                case 0:
                    {
                        db = RedisCon.ConnTrader.GetDatabase(database);
                        var st = await db.StringGetAsync(key);
                        return st;
                    }
                    //case 1:
                    //    {
                    //        db = RedisCon.ConnCommonfr.GetDatabase(database);
                    //        var members = await db.SetMembersAsync(key);
                    //        return await db.MGetAsync<T>(members.Select(x => x.ToString()));
                    //    }
                    //case 2:
                    //    {
                    //        db = RedisCon.ConnCommonfrnew.GetDatabase(database);
                    //        var members = await db.SetMembersAsync(key);
                    //        return await db.MGetAsync<T>(members.Select(x => x.ToString()));
                    //    }
                    //case 3:
                    //    {
                    //        _cacheManagerOther.UpdateSetMember(t1.FirstOrDefault().gmid.ToString(), "", redis);
                    //        break;
                    //    }
            }
            return null;
        }
        public bool IsExist(string key, int database = 0)
        {
            IDatabase db;
            db = RedisCon.ConnTrader.GetDatabase(database);
            if (db.KeyExists(key))
                return true;
            else
                return false;

        }
        #endregion
    }
}
