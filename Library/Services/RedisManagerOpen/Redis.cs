using Newtonsoft.Json;
using Services.RedisManager;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Services.RedisManagerOpen
{

    public enum RedisServer
    {
        Local = 1,
    }

    public class Redis : IRedis
    {
        public Redis()
        {
        }
        #region Methods
        public async Task<string> GetSetMembersstr(string key, int database, int re)
        {
            IDatabaseAsync db;
            switch (re)
            {
                case 0:
                    {
                        db = RedisCon.ConnLocal.GetDatabase(database);
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
            db = RedisCon.ConnLocal.GetDatabase(database);
            if (db.KeyExists(key))
                return true;
            else
                return false;

        }
        #endregion
    }
}
