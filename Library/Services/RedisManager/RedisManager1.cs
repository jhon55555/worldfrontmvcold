using Newtonsoft.Json;
using Common;
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

    public class RedisManager1 : IRedisManager1
    {
        private readonly RedisConnection1 _RedisCon;
        private readonly ConfigItems _config;
        public RedisManager1(RedisConnection1 RedisCon, ConfigItems config)
        {
            _RedisCon = RedisCon;
            _config = config;
        }
        #region Methods

        public void Set<T>(string key, int expireTime, T cacheItem, int serverr = 1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMilliseconds(expireTime));
            //    return;
            //}
        }

        public void SetInMin<T>(string key, int expireTime, T cacheItem, int serverr = 1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), TimeSpan.FromMinutes(expireTime));
            //    return;
            //}
        }
        public void Set<T>(string key, TimeSpan expireTime, T cacheItem, int serverr = 1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    db.StringSet(key, JsonConvert.SerializeObject(cacheItem), expireTime);

            //}
        }

        public T Get<T>(string key, int serverr = -1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));

            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                Boolean dd7 = db.StringGet(key).IsNull;
                if (!dd7)
                    return JsonConvert.DeserializeObject<T>(db.StringGet(key));
                else
                    return JsonConvert.DeserializeObject<T>("");
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                Boolean dd2 = db.StringGet(key).IsNull;
                if (!dd2)
                    return JsonConvert.DeserializeObject<T>(db.StringGet(key));
                else
                    return JsonConvert.DeserializeObject<T>("");
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                Boolean dd3 = db.StringGet(key).IsNull;
                if (!dd3)
                    return JsonConvert.DeserializeObject<T>(db.StringGet(key));
                else
                    return JsonConvert.DeserializeObject<T>("");
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                Boolean dd4 = db.StringGet(key).IsNull;
                if (!dd4)
                    return JsonConvert.DeserializeObject<T>(db.StringGet(key));
                else
                    return JsonConvert.DeserializeObject<T>("");
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    Boolean dd5 = db.StringGet(key).IsNull;
            //    if (!dd5)
            //        return JsonConvert.DeserializeObject<T>(db.StringGet(key));
            //    else
            //        return JsonConvert.DeserializeObject<T>("");
            //}
            else
            {
                db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisStaC);
                Boolean dd6 = db.StringGet(key).IsNull;
                if (!dd6)
                    return JsonConvert.DeserializeObject<T>(db.StringGet(key));
                else
                    return JsonConvert.DeserializeObject<T>("");
            }
            //var dd = db.StringGet(key).IsNull;
            //if (dd == true)
            //{
            //   var ddddd= key;
            //    string j2 = string.Join(",", key);
            //}
            //else
            //{
            //    return JsonConvert.DeserializeObject<T>(db.StringGet(key));

            //}

        }


        public bool IsExist(string key, int serverr = -1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                if (db.KeyExists(key))
                    return true;
                else
                    return false;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                if (db.KeyExists(key))
                    return true;
                else
                    return false;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                if (db.KeyExists(key))
                    return true;
                else
                    return false;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                if (db.KeyExists(key))
                    return true;
                else
                    return false;
            }
            else
            {
                ConfigItems.Redd = ConfigItems.RedisServerSta;
                db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisStaC);
                if (db.KeyExists(key))
                    return true;
                else
                    return false;
            }
            //if (db.KeyExists(key))
            //    return true;
            //else
            //    return false;
        }
        //public bool IsExistDyn(string key, int serverr = 1)
        //{
        //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
        //    IDatabase db;
        //    ConfigItems.Redd = s[0];
        //    db = _RedisCon.ConnLocalDyn.GetDatabase(Convert.ToInt32(s[1]));
        //    //db = _RedisCon.ConnLocalDyn(s[0]).GetDatabase(Convert.ToInt32(s[1]));
        //    if (db.KeyExists(key))
        //        return true;
        //    else
        //        return false;
        //}

        public void Remove(string key, int serverr = 1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                db.KeyDelete(key);
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                db.KeyDelete(key);
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                db.KeyDelete(key);
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                db.KeyDelete(key);
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    db.KeyDelete(key);
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    db.KeyDelete(key);
            //    return;
            //}
        }
        //public void RemoveDyn(string key, int serverr = 1)
        //{
        //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
        //    ConfigItems.Redd = s[0];
        //    IDatabase db = _RedisCon.ConnLocalDyn.GetDatabase(Convert.ToInt32(s[1]));
        //    //IDatabase db = _RedisCon.ConnLocalDyn(s[0]).GetDatabase(Convert.ToInt32(s[1]));
        //    db.KeyDelete(key);
        //}

        public void RemoveByPatterns(string pattern = "", int serverr = 1)
        {

            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //    var keys = _RedisCon.ServerLocal.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
            //    foreach (var item in keys)
            //    {
            //        db.KeyDelete(item);
            //    }
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servercric.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var item in keys)
                {
                    db.KeyDelete(item);
                }
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverfoot.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var item in keys)
                {
                    db.KeyDelete(item);
                }
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servertenni.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var item in keys)
                {
                    db.KeyDelete(item);
                }
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverother.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var item in keys)
                {
                    db.KeyDelete(item);
                }
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: pattern + "*").ToArray();
            //    foreach (var item in keys)
            //    {
            //        db.KeyDelete(item);
            //    }
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.RedisDataBase, pattern: pattern + "*").ToArray();
            //    foreach (var item in keys)
            //    {
            //        db.KeyDelete(item);
            //    }
            //    return;
            //}

        }

        public IList<T> GetAllkeysData<T>(string pattern, int serverr = 1)
        {
            IDatabase db;
            IList<T> data = new List<T>();
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //    foreach (var item in pattern.TrimEnd(',').Split(','))
            //    {
            //        var keys = _RedisCon.ServerLocal.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
            //        foreach (var key1 in keys)
            //        //foreach (var key in pattern.TrimEnd(',').Split(','))
            //        {
            //            if (key1.ToString().Contains('_'))
            //            {
            //                Boolean dd1 = db.StringGet(key1).IsNull;
            //                if (!dd1)
            //                    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(key1)));
            //            }
            //        }
            //    }
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                foreach (var item in pattern.TrimEnd(',').Split(','))
                {
                    var keys = _RedisCon.Servercric.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
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
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                foreach (var item in pattern.TrimEnd(',').Split(','))
                {
                    var keys = _RedisCon.Serverfoot.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
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
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                foreach (var item in pattern.TrimEnd(',').Split(','))
                {
                    var keys = _RedisCon.Servertenni.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
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
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                foreach (var item in pattern.TrimEnd(',').Split(','))
                {
                    var keys = _RedisCon.Serverother.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
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
            return data;

            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    foreach (var item in pattern.TrimEnd(',').Split(','))
            //    {
            //        var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: item + "*").ToArray();
            //        foreach (var key1 in keys)
            //        //foreach (var key in pattern.TrimEnd(',').Split(','))
            //        {
            //            if (key1.ToString().Contains('_'))
            //            {
            //                Boolean dd1 = db.StringGet(key1).IsNull;
            //                if (!dd1)
            //                    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(key1)));
            //            }
            //        }
            //    }
            //    return data;

            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    foreach (var item in pattern.TrimEnd(',').Split(','))
            //    {
            //        var keys = _RedisCon.ServerLocal.Keys(ConfigItems.RedisDataBase, pattern: item + "*").ToArray();
            //        foreach (var key1 in keys)
            //        //foreach (var key in pattern.TrimEnd(',').Split(','))
            //        {
            //            if (key1.ToString().Contains('_'))
            //            {
            //                Boolean dd1 = db.StringGet(key1).IsNull;
            //                if (!dd1)
            //                    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(key1)));
            //            }
            //        }
            //    }
            //    return data;

            //}
        }
        public void Delete(string pattern, int serverr = 1)
        {
            IDatabase db;
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //    var keys = _RedisCon.ServerLocal.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
            //    if (keys != null && keys.Count() == 1)
            //    {
            //        if (keys.FirstOrDefault().ToString() == pattern)
            //        {
            //            db.KeyDelete(pattern);
            //        }
            //    }
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servercric.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                if (keys != null && keys.Count() == 1)
                {
                    if (keys.FirstOrDefault().ToString() == pattern)
                    {
                        db.KeyDelete(pattern);
                    }
                }
                return;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverfoot.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                if (keys != null && keys.Count() == 1)
                {
                    if (keys.FirstOrDefault().ToString() == pattern)
                    {
                        db.KeyDelete(pattern);
                    }
                }
                return;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servertenni.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                if (keys != null && keys.Count() == 1)
                {
                    if (keys.FirstOrDefault().ToString() == pattern)
                    {
                        db.KeyDelete(pattern);
                    }
                }
                return;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverother.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                if (keys != null && keys.Count() == 1)
                {
                    if (keys.FirstOrDefault().ToString() == pattern)
                    {
                        db.KeyDelete(pattern);
                    }
                }
                return;
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: pattern + "*").ToArray();
            //    if (keys != null && keys.Count() == 1)
            //    {
            //        if (keys.FirstOrDefault().ToString() == pattern)
            //        {
            //            db.KeyDelete(pattern);
            //        }
            //    }
            //    return;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: pattern + "*").ToArray();
            //    if (keys != null && keys.Count() == 1)
            //    {
            //        if (keys.FirstOrDefault().ToString() == pattern)
            //        {
            //            db.KeyDelete(pattern);
            //        }
            //    }
            //    return;
            //}
        }
        //public IList<T> GetAllkeysDataDyn<T>(string pattern, int serverr = 1)
        //{
        //    //IDatabase db = _RedisCon.ConnLocal.GetDatabase(database);
        //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
        //    ConfigItems.Redd = s[0];
        //    IDatabase db = _RedisCon.ConnLocalDyn.GetDatabase(Convert.ToInt32(s[1]));
        //    //IDatabase db = _RedisCon.ConnLocalDyn(s[0]).GetDatabase(Convert.ToInt32(s[1]));
        //    IList<T> data = new List<T>();

        //    foreach (var item in pattern.TrimEnd(',').Split(','))
        //    {
        //        var keys = _RedisCon.ServerOdds.Keys(Convert.ToInt32(s[1]), pattern: item + "*").ToArray();
        //        foreach (var key1 in keys)
        //        //foreach (var key in pattern.TrimEnd(',').Split(','))
        //        {
        //            if (key1.ToString().Contains('_'))
        //            {
        //                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(key1)));
        //            }
        //        }
        //    }
        //    return data;
        //}
        public IList<T> GetSingalkeyData<T>(string pattern, int serverr = 1)
        {
            IDatabase db;
            IList<T> data = new List<T>();
            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //    var keys = _RedisCon.ServerLocal.Keys(Convert.ToInt32(s[1]), pattern: pattern).ToArray();
            //    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servercric.Keys(Convert.ToInt32(s[1]), pattern: pattern).ToArray();
                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
                return data;
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverfoot.Keys(Convert.ToInt32(s[1]), pattern: pattern).ToArray();
                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
                return data;
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servertenni.Keys(Convert.ToInt32(s[1]), pattern: pattern).ToArray();
                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
                return data;
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverother.Keys(Convert.ToInt32(s[1]), pattern: pattern).ToArray();
                data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
                return data;
            }
            return data;

            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: pattern).ToArray();
            //    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
            //    return data;
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.RedisDataBase, pattern: pattern).ToArray();
            //    data.Add(JsonConvert.DeserializeObject<T>(db.StringGet(keys.ToString())));
            //    return data;
            //}

        }
        public IList<string> GetAllkeys(string pattern, int serverr = 1)
        {
            IDatabase db;
            IList<string> data = new List<string>();

            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    db = _RedisCon.ConnLocal.GetDatabase(Convert.ToInt32(s[1]));
            //    var keys = _RedisCon.ServerLocal.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
            //    foreach (var key in keys)
            //    {
            //        data.Add(key);
            //    }

            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conncric.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servercric.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var key in keys)
                {
                    data.Add(key);
                }
                return data;

            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connfoot.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverfoot.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var key in keys)
                {
                    data.Add(key);
                }
                return data;

            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Conntenni.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Servertenni.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var key in keys)
                {
                    data.Add(key);
                }
                return data;

            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                db = _RedisCon.Connother.GetDatabase(Convert.ToInt32(s[1]));
                var keys = _RedisCon.Serverother.Keys(Convert.ToInt32(s[1]), pattern: pattern + "*").ToArray();
                foreach (var key in keys)
                {
                    data.Add(key);
                }
                return data;

            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.CasinoDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.CasinoDataBase, pattern: pattern + "*").ToArray();
            //    foreach (var key in keys)
            //    {
            //        data.Add(key);
            //    }
            //    return data;

            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    db = _RedisCon.ConnLocal.GetDatabase(ConfigItems.RedisDataBase);
            //    var keys = _RedisCon.ServerLocal.Keys(ConfigItems.RedisDataBase, pattern: pattern + "*").ToArray();
            //    foreach (var key in keys)
            //    {
            //        data.Add(key);
            //    }
            //    return data;

            //}
            return data;

        }
        public void Flush(int serverr = 1)
        {

            //if (serverr > -1)
            //{
            //    var s = ConfigItems.RedisServerSta1[serverr].Split('|');
            //    ConfigItems.Redd = s[0];
            //    _RedisCon.ServerLocal.FlushDatabase(Convert.ToInt32(s[1]));

            //}
            if (serverr == 0)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                _RedisCon.Servercric.FlushAllDatabases();
            }
            if (serverr == 1)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                _RedisCon.Serverfoot.FlushDatabase(Convert.ToInt32(s[1]));
            }
            if (serverr == 2)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                _RedisCon.Servertenni.FlushDatabase(Convert.ToInt32(s[1]));
            }
            if (serverr == 3)
            {
                var s = ConfigItems.RedisServerSta1[serverr].Split('|');
                ConfigItems.Redd = s[0];
                _RedisCon.Serverother.FlushDatabase(Convert.ToInt32(s[1]));
            }
            //else if (serverr == -2)
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    _RedisCon.ServerLocal.FlushDatabase(ConfigItems.CasinoDataBase);
            //    _RedisCon.Serverother.FlushDatabase(Convert.ToInt32(s[1]));
            //}
            //else
            //{
            //    ConfigItems.Redd = ConfigItems.RedisServer;
            //    _RedisCon.ServerLocal.FlushDatabase(ConfigItems.RedisDataBase);
            //}
        }
        #endregion
    }
}
