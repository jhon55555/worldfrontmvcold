using Common;
using DimFrontDiamond.AutherizationAtteributes;
using DimFrontDiamond.Providers;
using ELog;
using Microsoft.Ajax.Utilities;
using Models.DimFront;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Services.CacheManager;
using Services.DimFront;
using Services.RedisManage;
using Services.RedisManager;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Data;
using System.IdentityModel.Tokens;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;

namespace DimFrontDiamond.Controllers
{
    [ModelValid]
    [RoutePrefix("api/wd")]
    public class FrontController : ApiController
    {
        #region Fields

        private readonly IDimFrontService _DimFrontDiamondservice;
        private readonly IRedisManager _cacheManager;
        //private readonly IRedisManager2 _cacheManager1;
        private readonly IRedisManagerCric _cacheManagerCric;
        private readonly IRedisManagerFoot _cacheManagerFoot;
        private readonly IRedisManagerTenni _cacheManagerTenni;
        private readonly IRedisManagerOther _cacheManagerOther;
        private readonly IRedisManagerTrader _redisManagerTrader;
        private readonly IRedis1 _cache;
        //private static readonly SemaphoreSlim _addEventLock = new(1, 1);

        #endregion

        #region Ctor

        public FrontController(IDimFrontService DimFrontDiamondservice, IRedisManager rediscache, IRedisManagerTrader redisManagerTrader, IRedisManagerCric redisManagerCric, IRedisManagerFoot redisManagerFoot, IRedisManagerTenni redisManagerTenni, IRedisManagerOther redisManagerOther, IRedis1 cache)
        {
            _DimFrontDiamondservice = DimFrontDiamondservice;
            _cacheManager = rediscache;
            _cacheManagerCric = redisManagerCric;
            _cacheManagerFoot = redisManagerFoot;
            _cacheManagerTenni = redisManagerTenni;
            _cacheManagerOther = redisManagerOther;
            _redisManagerTrader = redisManagerTrader;
            _cache = cache;
        }

        #endregion

        #region Methods

        public HttpResponseMessage Return100(String Msg)
        {
            return Request.CreateResponse(HttpStatusCode.OK, new { status = 100, msg = Msg });
        }
        public HttpResponseMessage Return200(String Msg, Object Data = null)
        {
            if (Data == null)
                return Request.CreateResponse(HttpStatusCode.OK, new { status = 200, msg = Msg });
            return Request.CreateResponse(HttpStatusCode.OK, new { status = 200, msg = Msg, data = Data });
        }
        public HttpResponseMessage Return300(String Msg)
        {
            return Request.CreateResponse(HttpStatusCode.OK, new { status = 300, msg = Msg });
        }
        public HttpResponseMessage Return400(String Msg)
        {
            return Request.CreateResponse(HttpStatusCode.OK, new { status = 400, msg = Msg });
        }
        public HttpResponseMessage Return401(String Msg)
        {
            return Request.CreateResponse(HttpStatusCode.OK, new { status = 401, msg = Msg });
        }
        private static async Task<long> GetValuesFunctionAsync(IDatabaseAsync db, RedisKey[] in_redis_keys, RedisValue[] out_values, int max_array_size = 5000)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            int sourceIndex = 0;
            int count = in_redis_keys.Count();
            RedisKey[] redis_keys_tmp_prepared = new RedisKey[max_array_size];

            while (count > 0)
            {
                int array_count = (count - max_array_size) < 0 ? count : max_array_size;
                count -= array_count;

                RedisKey[] redis_keys_tmp = redis_keys_tmp_prepared;

                if (array_count != max_array_size)
                {
                    redis_keys_tmp = new RedisKey[array_count];
                }

                Array.Copy(in_redis_keys, sourceIndex, redis_keys_tmp, 0, array_count);

                RedisValue[] string_get_result = await db.StringGetAsync(redis_keys_tmp);

                Array.Copy(string_get_result, 0, out_values, sourceIndex, array_count);

                sourceIndex += array_count;

                // if (count > 0) // effect too low
                //    System.Threading.Thread.Sleep(4);
            }

            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        private static long GetValuesFunction(IDatabase db, RedisKey[] in_redis_keys, RedisValue[] out_values, int max_array_size = 5000)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            int sourceIndex = 0;
            int count = in_redis_keys.Count();
            RedisKey[] redis_keys_tmp_prepared = new RedisKey[max_array_size];

            while (count > 0)
            {
                int array_count = (count - max_array_size) < 0 ? count : max_array_size;
                count -= array_count;

                RedisKey[] redis_keys_tmp = redis_keys_tmp_prepared;

                if (array_count != max_array_size)
                {
                    redis_keys_tmp = new RedisKey[array_count];
                }

                Array.Copy(in_redis_keys, sourceIndex, redis_keys_tmp, 0, array_count);

                RedisValue[] string_get_result = db.StringGet(redis_keys_tmp);

                Array.Copy(string_get_result, 0, out_values, sourceIndex, array_count);

                sourceIndex += array_count;

                // if (count > 0) // effect too low
                //    System.Threading.Thread.Sleep(4);
            }

            watch.Stop();

            return watch.ElapsedMilliseconds;
        }

        // select all data once by optimized function using GetValuesFunction internally
        // mainly used in highlight data
        private static long Get_redis_query_dictionary_by_db(
                dynamic _cacheManager,
                IEnumerable<dynamic> dataetid1,
                IEnumerable<dynamic> dataetid2,
                Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db
        //               database           key     value
        )
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            if (dataetid1 != null)
            {
                foreach (var item in dataetid1)
                {
                    var k = item;
                    if (!redis_query_dictionary_by_db.ContainsKey(k.m))
                    {
                        redis_query_dictionary_by_db[k.m] = new Dictionary<string, string>();
                    }
                    redis_query_dictionary_by_db[k.m][k.gmid.ToString()] = null;
                    redis_query_dictionary_by_db[k.m][k.mid.ToString()] = null;
                }
            }
            if (dataetid2 != null)
            {
                foreach (var item in dataetid2)
                {
                    var k = item;
                    if (!redis_query_dictionary_by_db.ContainsKey(k.m))
                    {
                        redis_query_dictionary_by_db[k.m] = new Dictionary<string, string>();
                    }
                    redis_query_dictionary_by_db[k.m][k.gmid.ToString()] = null;
                    redis_query_dictionary_by_db[k.m][k.mid.ToString()] = null;
                }
            }

            foreach (var redis_query_dictionary_item in redis_query_dictionary_by_db)
            {
                // first select a DB
                IDatabase _db = _cacheManager.getDb(redis_query_dictionary_item.Key);

                // then select a dictionary to write data to
                Dictionary<string, string> redis_query_dictionary = redis_query_dictionary_item.Value;

                RedisKey[] redis_keys = redis_query_dictionary.Select(item => (RedisKey)item.Key).ToArray();
                RedisValue[] redis_values = new RedisValue[redis_keys.Count()];

                // linear optimized operation - get values from Redis
                GetValuesFunction(_db, redis_keys, redis_values);

                // write result into dictionary
                for (var i = 0; i < redis_values.Count(); i++)
                {
                    string key = redis_keys[i];
                    string data = redis_values[i];
                    if (key == null || data == null)
                    {
                        continue; // that's normal - didn't found anything
                    }
                    redis_query_dictionary[key] = data;
                }
            }

            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        [HttpGet]
        [Route("~/test")]
        public HttpResponseMessage Test()
        {
            //var v = Assembly.GetExecutingAssembly().GetName().Version;
            return Return200("v:1.1");
        }

        [HttpPost]
        [Route("highlightdata")]
        public async Task<HttpResponseMessage> HighlightData(HighlightData highlightData)
        {
            try
            {
                ErrorLog.WriteLogAll("HighlightData", JsonConvert.SerializeObject(highlightData));
                Boolean ren = _redisManagerTrader.IsExist("M", ConfigItems.Rediscdb);
                if (!ren)
                {
                    var par = "etid=" + highlightData.etid;
                    var resp = HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.getm, par, "application/x-www-form-urlencoded", "POST");
                }
                var gdata = _redisManagerTrader.Get<GetMre>("M", ConfigItems.Rediscdb);
                highlightData.m = gdata.t1.FirstOrDefault().m;
                if (highlightData.m == 0)
                {

                    if (!_cacheManagerCric.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                    {
                        var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                        await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                    }
                    string vid = highlightData.etid.ToString();
                    if (highlightData.etid == 999)
                    {
                        highlightData.etid = 4;
                    }
                    var data = _cacheManagerCric.Get<HighlightData11>("HighlightData_" + highlightData.etid, highlightData.m);
                    try
                    {
                        if (data != null)
                        {
                            if (highlightData.action.ToLower() == "all")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString());
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerCric, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = /*dataetid1 != null && dataetid1.Count() > 0 ?*/ dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList() /*: null*/;

                                    var mdata1 = /*dataetid2 != null && dataetid2.Count() > 0 ?*/ dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList() /*: null*/;

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       oid = pd.oid,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         oid = pd.oid,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (highlightData.etid == 4 && vid == "999")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc != 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc != 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    if (highlightData.etid == 4 && vid == "4")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else if (highlightData.action.ToLower() == "today")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.stime.ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"));
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerCric, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();
                                    //var mdata = dataetid1.Where(k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       oid = pd.oid,
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         pd.detail.m,
                                                                                         oid = pd.oid,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    //return Return200("Success", t2);if (etid == "4")
                                    if (highlightData.etid == 4 && vid == "999")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc != 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc != 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    if (highlightData.etid == 4 && vid == "4")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }

                            }
                            else if (highlightData.action.ToLower() == "inplay")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.iplay == true);
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    //keyid = string.Join(",", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerCric, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    //var mdata = dataetid1.Where(k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         oid = pd.oid,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (highlightData.etid == 4 && vid == "999")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc != 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc != 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    if (highlightData.etid == 4 && vid == "4")
                                    {
                                        var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                        var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                        if (t3.Count() == 0 && t4.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = t3, t2 = t4 });
                                    }
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else
                            {
                                if (!_cacheManagerCric.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                                {
                                    var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                                    await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                                    return Return300("No record found.");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ErrorLog.WriteLog("HighlightDatacric", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                        return Return300("invalid data.");
                    }
                }
                if (highlightData.m == 1)
                {
                    if (!_cacheManagerFoot.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                    {
                        var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                        await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                    }
                    var data = _cacheManagerFoot.Get<HighlightData11>("HighlightData_" + highlightData.etid, highlightData.m);
                    try
                    {
                        if (data != null)
                        {
                            if (highlightData.action.ToLower() == "all")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString());
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerFoot, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       oid = pd.oid,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         oid = pd.oid,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else if (highlightData.action.ToLower() == "today")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.stime.ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"));
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerFoot, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();
                                    //var mdata = dataetid1.Where(k => _cacheManagerFoot.IsExist(k.gmid.ToString(), k.m) && _cacheManagerFoot.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerFoot.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerFoot.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerFoot.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerFoot.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       oid = pd.oid,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         oid = pd.oid,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }

                            }
                            else if (highlightData.action.ToLower() == "inplay")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.iplay == true);
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    //keyid = string.Join(",", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerFoot, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();
                                    //var mdata = dataetid1.Where(k => _cacheManagerFoot.IsExist(k.gmid.ToString(), k.m) && _cacheManagerFoot.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerFoot.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerFoot.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerFoot.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerFoot.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       oid = pd.oid,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         oid = pd.oid,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else
                            {
                                if (!_cacheManagerFoot.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                                {
                                    var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                                    await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                                }
                                return Return300("No record found.");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ErrorLog.WriteLog("HighlightDatafoot", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                        return Return300("invalid data.");
                    }
                }
                if (highlightData.m == 2)
                {
                    if (!_cacheManagerTenni.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                    {
                        var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                        await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                    }
                    var data = _cacheManagerTenni.Get<HighlightData11>("HighlightData_" + highlightData.etid, highlightData.m);
                    try
                    {
                        if (data != null)
                        {
                            if (highlightData.action.ToLower() == "all")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString());
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerTenni, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       oid = pd.oid,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         oid = pd.oid,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else if (highlightData.action.ToLower() == "today")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.stime.ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"));
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    //var mdata = dataetid1.Where(k => _cacheManagerTenni.IsExist(k.gmid.ToString(), k.m) && _cacheManagerTenni.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerTenni.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerTenni.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerTenni.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerTenni.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerTenni, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         oid = pd.oid,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    //return Return200("Success", t2);if (etid == "4")
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }

                            }
                            else if (highlightData.action.ToLower() == "inplay")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.iplay == true);
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    //keyid = string.Join(",", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    //var mdata = dataetid1.Where(k => _cacheManagerTenni.IsExist(k.gmid.ToString(), k.m) && _cacheManagerTenni.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerTenni.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerTenni.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    oid = p.oid,
                                    //    m = p.m,
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerTenni.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerTenni.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerTenni, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         oid = pd.oid,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else
                            {
                                if (!_cacheManagerTenni.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                                {
                                    var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                                    await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                                }
                                return Return300("No record found.");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ErrorLog.WriteLog("HighlightDatatenni", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                        return Return300("invalid data.");
                    }
                }
                if (highlightData.m == 3)
                {
                    if (!_cacheManagerOther.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                    {
                        var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                        await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                    }
                    var data = _cacheManagerOther.Get<HighlightData11>("HighlightData_" + highlightData.etid, highlightData.m);
                    try
                    {
                        if (data != null)
                        {
                            if (highlightData.action.ToLower() == "all")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString());
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerOther, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         oid = pd.oid,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
                                    {
                                        var query = t1.GroupBy(r1 => new
                                        {
                                            r1.cid,
                                            r1.cname,
                                        }, (key1, group1) => new
                                        {
                                            cid = key1.cid,
                                            cname = key1.cname,
                                            children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                            {
                                                r2.ename,
                                            }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                            {
                                                ename = key2.ename,
                                                children = group2.Select(y => new
                                                {
                                                    etid = y.etid,
                                                    gmid = y.gmid,
                                                    iplay = y.iplay,
                                                    stime = y.stime,
                                                    m = y.m,
                                                    gtype = y.gtype
                                                }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                            }).ToList(),
                                            //)
                                        }).ToList();
                                        //var query1 = t2.GroupBy(r1 => new
                                        //{
                                        //    r1.cid,
                                        //    r1.cname,
                                        //}, (key1, group1) => new
                                        //{
                                        //    cid = key1.cid,
                                        //    cname = key1.cname,
                                        //    children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                        //    {
                                        //        r2.ename,
                                        //    }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                        //    {
                                        //        ename = key2.ename,
                                        //        children = group2.Select(y => new
                                        //        {
                                        //            gmid = y.gmid,
                                        //            iplay = y.iplay,
                                        //            stime = y.stime,
                                        //        }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                        //    }).ToList(),
                                        //    //)
                                        //}).ToList();
                                        if (query.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = query });
                                    }
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else if (highlightData.action.ToLower() == "today")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.stime.ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"));
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();

                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerOther, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();
                                    //var mdata = dataetid1.Where(k => _cacheManagerOther.IsExist(k.gmid.ToString(), k.m) && _cacheManagerOther.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerOther.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerOther.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerOther.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerOther.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         pd.detail.m,
                                                                                         oid = pd.oid,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
                                    {
                                        var query = t1.GroupBy(r1 => new
                                        {
                                            r1.cid,
                                            r1.cname,
                                        }, (key1, group1) => new
                                        {
                                            cid = key1.cid,
                                            cname = key1.cname,
                                            children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                            {
                                                r2.ename,
                                            }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                            {
                                                ename = key2.ename,
                                                children = group2.Select(y => new
                                                {
                                                    etid = y.etid,
                                                    gmid = y.gmid,
                                                    iplay = y.iplay,
                                                    stime = y.stime,
                                                    m = y.m,
                                                    gtype = y.gtype
                                                }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                            }).ToList(),
                                            //)
                                        }).ToList();
                                        //var query1 = t2.GroupBy(r1 => new
                                        //{
                                        //    r1.cid,
                                        //    r1.cname,
                                        //}, (key1, group1) => new
                                        //{
                                        //    cid = key1.cid,
                                        //    cname = key1.cname,
                                        //    children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                        //    {
                                        //        r2.ename,
                                        //    }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                        //    {
                                        //        ename = key2.ename,
                                        //        children = group2.Select(y => new
                                        //        {
                                        //            gmid = y.gmid,
                                        //            iplay = y.iplay,
                                        //            stime = y.stime,
                                        //        }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                        //    }).ToList(),
                                        //    //)
                                        //}).ToList();
                                        if (query.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = query });
                                    }
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }

                            }
                            else if (highlightData.action.ToLower() == "inplay")
                            {
                                var dataetid = data.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.iplay == true);
                                if (dataetid != null && dataetid.Count() > 0)
                                {
                                    //keyid = string.Join(",", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                    var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                    var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                    Dictionary<int, Dictionary<string, string>> redis_query_dictionary_by_db = new Dictionary<int, Dictionary<string, string>>();
                                    //       database           key     value

                                    Get_redis_query_dictionary_by_db(_cacheManagerOther, dataetid1, dataetid2, redis_query_dictionary_by_db);

                                    var mdata = dataetid1.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m) && _cacheManagerCric.IsExist(k.mid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) && // k.m is a databse
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()]) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.mid.ToString()])
                                    )
                                    .Select(
                                        p => new
                                        {
                                                // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                                // gdata = _cacheManagerCric.Get<GameDataw>(p.mid.ToString(), p.m),
                                                detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                            gdata = JsonConvert.DeserializeObject<GameDataw>(redis_query_dictionary_by_db[p.m][p.mid.ToString()]),
                                            oid = p.oid
                                        }
                                    )
                                    .ToList();

                                    var mdata1 = dataetid2.Where(
                                        // k => _cacheManagerCric.IsExist(k.gmid.ToString(), k.m)
                                        k => redis_query_dictionary_by_db.ContainsKey(k.m) &&
                                             !String.IsNullOrEmpty(redis_query_dictionary_by_db[k.m][k.gmid.ToString()])
                                    )
                                    .Select(p => new
                                    {
                                            // detail = _cacheManagerCric.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                            detail = JsonConvert.DeserializeObject<GameMasterw>(redis_query_dictionary_by_db[p.m][p.gmid.ToString()]),
                                        oid = p.oid
                                    }
                                    )
                                    .ToList();
                                    //var mdata = dataetid1.Where(k => _cacheManagerOther.IsExist(k.gmid.ToString(), k.m) && _cacheManagerOther.IsExist(k.mid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerOther.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    gdata = _cacheManagerOther.Get<GameDataw>(p.mid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();
                                    //var mdata1 = dataetid2.Where(k => _cacheManagerOther.IsExist(k.gmid.ToString(), k.m)).Select(p => new
                                    //{
                                    //    detail = _cacheManagerOther.Get<GameMasterw>(p.gmid.ToString(), p.m),
                                    //    m = p.m,
                                    //    oid = p.oid
                                    //}).ToList();

                                    var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                   join rt in mdata.Select(o => o.gdata)
                                                                                   on pd.detail.gmid equals rt.gmid
                                                                                   orderby pd.detail.gmid
                                                                                   select new
                                                                                   {
                                                                                       pd.detail.gmid,
                                                                                       pd.detail.ename,
                                                                                       pd.detail.etid,
                                                                                       pd.detail.cid,
                                                                                       pd.detail.cname,
                                                                                       pd.detail.iplay,
                                                                                       stime = pd.detail.stime.ToString(),
                                                                                       pd.detail.tv,
                                                                                       pd.detail.bm,
                                                                                       pd.detail.f,
                                                                                       pd.detail.f1,
                                                                                       pd.detail.iscc,
                                                                                       oid = pd.oid,
                                                                                       mid = rt == null ? 0 : rt.mid,
                                                                                       mname = rt == null ? "" : rt.mname,
                                                                                       status = rt == null ? "" : rt.status,
                                                                                       rc = rt == null ? 0 : rt.rc,
                                                                                       gscode = rt == null ? 0 : rt.gscode,
                                                                                       m = pd == null ? 0 : pd.detail.m,
                                                                                       gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                       section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                                   }).DistinctBy(w => w.mid).ToList() : null;
                                    var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata1.Select(o => new { detail = o.detail.t1.FirstOrDefault(), oid = o.oid })
                                                                                     orderby pd.detail.gmid
                                                                                     select new
                                                                                     {
                                                                                         pd.detail.gmid,
                                                                                         pd.detail.ename,
                                                                                         pd.detail.etid,
                                                                                         pd.detail.cid,
                                                                                         pd.detail.cname,
                                                                                         pd.detail.iplay,
                                                                                         stime = pd.detail.stime.ToString(),
                                                                                         pd.detail.tv,
                                                                                         pd.detail.bm,
                                                                                         pd.detail.f,
                                                                                         pd.detail.f1,
                                                                                         pd.detail.iscc,
                                                                                         oid = pd.oid,
                                                                                         mid = 0,
                                                                                         mname = "MATCH_ODDS",
                                                                                         status = "SUSPENDED",
                                                                                         rc = 2,
                                                                                         gscode = 0,
                                                                                         m = pd == null ? 0 : pd.detail.m,
                                                                                         gtype = "match",
                                                                                         section = new object[]
                                                                                         {
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 1,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               },
                                                                                                       new{
                                                                                                       sid = 0,
                                                                                                       gstatus = "SUSPENDED",
                                                                                                       nat = "",
                                                                                                       sno = 3,
                                                                                                       gscode = 0,
                                                                                                       odds = new object[]
                                                                                                  {
                                                                                                       new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "BACK1",
                                                                                                       otype = "BACK",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   },
                                                                                                     new{
                                                                                                       odds = 0.0,
                                                                                                       oname = "LAY1",
                                                                                                       otype = "LAY",
                                                                                                       sid = 0,
                                                                                                       tno = 0.0,
                                                                                                       size = 0.0
                                                                                                   }
                                                                                                  }
                                                                                               }
                                                                                         }
                                                                                     }).ToList() : null;
                                    if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
                                    {
                                        var query = t1.GroupBy(r1 => new
                                        {
                                            r1.cid,
                                            r1.cname,
                                        }, (key1, group1) => new
                                        {
                                            cid = key1.cid,
                                            cname = key1.cname,
                                            children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                            {
                                                r2.ename,
                                            }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                            {
                                                ename = key2.ename,
                                                children = group2.Select(y => new
                                                {
                                                    etid = y.etid,
                                                    gmid = y.gmid,
                                                    iplay = y.iplay,
                                                    stime = y.stime,
                                                    m = y.m,
                                                    gtype = y.gtype
                                                }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                            }).ToList(),
                                            //)
                                        }).ToList();
                                        //var query1 = t2.GroupBy(r1 => new
                                        //{
                                        //    r1.cid,
                                        //    r1.cname,
                                        //}, (key1, group1) => new
                                        //{
                                        //    cid = key1.cid,
                                        //    cname = key1.cname,
                                        //    children = group1.Count() == 0 ? null :/*group1.Select(_ => _.cid == "" ? null :*/ group1.GroupBy(r2 => new
                                        //    {
                                        //        r2.ename,
                                        //    }, (key2, group2) => key2.ename.ToString() == "" ? null : new
                                        //    {
                                        //        ename = key2.ename,
                                        //        children = group2.Select(y => new
                                        //        {
                                        //            gmid = y.gmid,
                                        //            iplay = y.iplay,
                                        //            stime = y.stime,
                                        //        }).OrderBy(i => Convert.ToDateTime(i.stime)).ToList(),
                                        //    }).ToList(),
                                        //    //)
                                        //}).ToList();
                                        if (query.Count() == 0)
                                            return Return300("No record found.");
                                        return Return200("Success", new { t1 = query });
                                    }
                                    if (t1.Count() == 0 && t2.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t1, t2 = t2 });
                                }
                            }
                            else
                            {
                                if (!_cacheManagerOther.IsExist("HighlightData_" + highlightData.etid, highlightData.m))
                                {
                                    var par = "etid=" + highlightData.etid.ToString() + "&action=" + highlightData.action.ToString() + "&m=" + highlightData.m;
                                    await Task.Factory.StartNew(() => HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.highlightdata, par, "application/x-www-form-urlencoded", "POST"));
                                }
                                return Return300("No record found.");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ErrorLog.WriteLog("HighlightDataother", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                        return Return300("invalid data.");
                    }
                }
                return Return300("No record found");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("HighlightData", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                return Return400(ex.Message);
            }

        }
        [HttpPost]
        [Route("gamedetail")]
        public async Task<HttpResponseMessage> GameDetail(GameDetail gameDetail)
        {
            try
            {
                ErrorLog.WriteLogAll("GameDetail", JsonConvert.SerializeObject(gameDetail));
                //string u_id = string.Empty;
                //var header = HttpContext.Current.Request.Headers["Authorization"].ToString();
                //if (header != null)
                //{
                //    u_id = JwtTokenGenerator.GetSaGuid(header);
                //}

                //Guid uu_id;
                //bool res = Guid.TryParse(u_id, out uu_id);

                //if (!res)
                //    return Return300("token not valid.");

                //bool uredis = JwtTokenGenerator.Geturedis(header);
                //if (uredis)
                //{
                //Boolean ren = _redisManagerTrader.IsExist("M", ConfigItems.Rediscdb);
                //if (!ren)
                //{
                //    var par = "etid=" + gameDetail.etid;
                //    var resp = HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.getm, par, "application/x-www-form-urlencoded", "POST");
                //}
                //if (ren)
                //{
                //var gdata = _redisManagerTrader.Get<GetMre>("M", ConfigItems.Rediscdb);
                //if (gdata != null && gdata.t1 != null && gdata.t1.Count() > 0)
                //{
                if (gameDetail.etid == 999)
                    gameDetail.etid = 4;

                //string objss = gdata.t1.Where(_ => _.etid == gameDetail.etid).FirstOrDefault().m.ToString();
                //Int32 obj = 0;
                //if (!string.IsNullOrEmpty(objss))
                //    obj = Convert.ToInt32(objss);
                //int uid = 0;
                //uid = JwtTokenGenerator.GetUid(header);
                //uid = uid % 10;
                //uid = uid + 90;
                if (gameDetail.etid == 4)
                {
                    var masterdata = await _cacheManagerCric.Getasync<GameMaster>(gameDetail.gmid.ToString(), 0);
                    if (masterdata != null && masterdata.t1 != null)
                    {
                        masterdata.t1.FirstOrDefault().gtype = masterdata.t1.FirstOrDefault().gtype.ToLower();
                        if (masterdata.t1.FirstOrDefault().scard == 1)
                        {
                            var par1 = new
                            {
                                newgameid = gameDetail.gmid.ToString()
                            };
                            //var par1 = "{\"newgameid\":\"" + gameDetail.gmid.ToString() + "\"}";
                            var resp1 = HttpHelper.Post(ConfigItems.Urlsc + ApiEndpoint.getoldgameid, JsonConvert.SerializeObject(par1), "application/json", "POST");
                            var obj1 = JsonConvert.DeserializeObject<Gamenewid>(resp1);
                            masterdata.t1.FirstOrDefault().oldgmid = obj1 != null && obj1.status == 200 ? obj1.gameid : "0";
                        }
                        //masterdata.t1.FirstOrDefault().port = uid;
                        var data = masterdata.t1.Select(d => d);
                        return Return200("Success", data);
                    }
                }
                if (gameDetail.etid == 1)
                {
                    var masterdata = await _cacheManagerFoot.Getasync<GameMaster>(gameDetail.gmid.ToString(), gameDetail.etid);
                    if (masterdata != null && masterdata.t1 != null)
                    {
                        masterdata.t1.FirstOrDefault().gtype = masterdata.t1.FirstOrDefault().gtype.ToLower();
                        if (masterdata.t1.FirstOrDefault().scard == 1)
                        {
                            var par1 = new
                            {
                                newgameid = gameDetail.gmid.ToString()
                            };
                            //var par1 = "{\"newgameid\":\"" + gameDetail.gmid.ToString() + "\"}";
                            var resp1 = HttpHelper.Post(ConfigItems.Urlsc + ApiEndpoint.getoldgameid, JsonConvert.SerializeObject(par1), "application/json", "POST");
                            var obj1 = JsonConvert.DeserializeObject<Gamenewid>(resp1);
                            masterdata.t1.FirstOrDefault().oldgmid = obj1 != null && obj1.status == 200 ? obj1.gameid : "0";
                        }
                        //masterdata.t1.FirstOrDefault().port = uid;
                        var data = masterdata.t1.Select(d => d);
                        return Return200("Success", data);
                    }
                }
                if (gameDetail.etid == 2)
                {
                    var masterdata = await _cacheManagerTenni.Getasync<GameMaster>(gameDetail.gmid.ToString(), gameDetail.etid);
                    if (masterdata != null && masterdata.t1 != null)
                    {
                        masterdata.t1.FirstOrDefault().gtype = masterdata.t1.FirstOrDefault().gtype.ToLower();
                        if (masterdata.t1.FirstOrDefault().scard == 1)
                        {
                            var par1 = new
                            {
                                newgameid = gameDetail.gmid.ToString()
                            };
                            //var par1 = "{\"newgameid\":\"" + gameDetail.gmid.ToString() + "\"}";
                            var resp1 = HttpHelper.Post(ConfigItems.Urlsc + ApiEndpoint.getoldgameid, JsonConvert.SerializeObject(par1), "application/json", "POST");
                            var obj1 = JsonConvert.DeserializeObject<Gamenewid>(resp1);
                            masterdata.t1.FirstOrDefault().oldgmid = obj1 != null && obj1.status == 200 ? obj1.gameid : "0";
                        }
                        //masterdata.t1.FirstOrDefault().port = uid;
                        var data = masterdata.t1.Select(d => d);
                        return Return200("Success", data);
                    }
                }
                else
                {
                    var masterdata = await _cacheManagerOther.Getasync<GameMaster>(gameDetail.gmid.ToString(), 0);
                    if (masterdata != null && masterdata.t1 != null)
                    {
                        masterdata.t1.FirstOrDefault().gtype = masterdata.t1.FirstOrDefault().gtype.ToLower();
                        if (masterdata.t1.FirstOrDefault().scard == 1)
                        {
                            var par1 = new
                            {
                                newgameid = gameDetail.gmid.ToString()
                            };
                            //var par1 = "{\"newgameid\":\"" + gameDetail.gmid.ToString() + "\"}";
                            var resp1 = HttpHelper.Post(ConfigItems.Urlsc + ApiEndpoint.getoldgameid, JsonConvert.SerializeObject(par1), "application/json", "POST");
                            var obj1 = JsonConvert.DeserializeObject<Gamenewid>(resp1);
                            masterdata.t1.FirstOrDefault().oldgmid = obj1 != null && obj1.status == 200 ? obj1.gameid : "0";
                        }

                        //masterdata.t1.FirstOrDefault().port = uid;
                        var data = masterdata.t1.Select(d => d);
                        return Return200("Success", data);
                    }
                }
                //}

                //}


                //var par = "gmid=" + gameDetail.gmid + "&etid=" + gameDetail.etid;
                //var resp = HttpHelper.Post(ConfigItems.RedisUrl + ApiEndpoint.gamedetail, par, "application/x-www-form-urlencoded", "POST");
                //var obj = JsonConvert.DeserializeObject<GameDetailRes>(resp);
                //if (obj != null && obj.data != null && obj.data.FirstOrDefault().scard == 1)
                //{
                //    var par1 = new
                //    {
                //        newgameid = gameDetail.gmid.ToString()
                //    };
                //    //var par1 = "{\"newgameid\":\"" + gameDetail.gmid.ToString() + "\"}";
                //    var resp1 = HttpHelper.Post(ConfigItems.Urlsc + ApiEndpoint.getoldgameid, JsonConvert.SerializeObject(par1), "application/json", "POST");
                //    var obj1 = JsonConvert.DeserializeObject<Gamenewid>(resp1);
                //    obj.data.FirstOrDefault().oldgmid = obj1 != null && obj1.status == 200 ? obj1.gameid : "0";
                //}
                //return obj;
                //}
                return Return300("No record found.");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("GameDetail", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(gameDetail));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("gamedata")]
        public async Task<HttpResponseMessage> GameData(GameData gameData)
        {
            try
            {
                ErrorLog.WriteLogAll("GameData", JsonConvert.SerializeObject(gameData));
                //string u_id = string.Empty;
                //var header = HttpContext.Current.Request.Headers["Authorization"].ToString();
                //if (header != null)
                //{
                //    u_id = JwtTokenGenerator.GetSaGuid(header);
                //}

                //Guid uu_id;
                //bool res = Guid.TryParse(u_id, out uu_id);

                //if (!res)
                //    return Return300("token not valid.");
                //bool uredis = JwtTokenGenerator.Geturedis(header);
                //if (uredis)
                //{
                if (gameData.m == 0)
                {
                    //               /* await _addEventLock.WaitAsync();*/
                    //                if (!await _cacheManagerCric.IsExistasync(gameData.gmid.ToString(), gameData.m))
                    //                {
                    //                    return Return200("Success", "");
                    //                }
                    //                 _addEventLock.Release();
                    var masterdata = await _cacheManagerCric.Getasync<GameMaster>(gameData.gmid.ToString(), gameData.m);
                    var gdata = await _cacheManagerCric.GetAllkeysData<GameDataw>(gameData.gmid.ToString(), gameData.m);
                    if (gdata != null && gdata.Count() > 0)
                    {
                        if (masterdata.t1.FirstOrDefault().iplay == true)
                        {
                            var data = gdata.Where(p => p.company.Any(
                                i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true && Convert.ToBoolean(p.biplay) == true
                                ).Select(l => new
                                {
                                    mid = l.mid,
                                    mname = l.mname,
                                    status = l.status,
                                    rc = l.rc,
                                    sno = l.sno,
                                    ocnt = l.ocnt,
                                    gscode = l.gscode,
                                    dtype = l.dtype,
                                    gtype = l.gtype.ToString().ToLower(),
                                    maxb = l.maxb,
                                    max = l.max,
                                    min = l.min,
                                    rem = l.rem,
                                    umaxbof = l.umaxbof,
                                    l.biplay,
                                    l.boplay,
                                    iplay = masterdata.t1.FirstOrDefault().iplay,
                                    section = l.section.Select(y => new
                                    {
                                        sid = y.sid,
                                        sno = y.sno,
                                        gstatus = y.gstatus,
                                        nat = y.nat,
                                        psrno = y.psrno,
                                        gscode = y.gscode,
                                        y.max,
                                        y.min,
                                        y.rem,
                                        odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i => new
                                        {
                                            i.odds,
                                            i.otype,
                                            i.oname,
                                            i.tno,
                                            size = i.odds == 0 ? 0 : i.size
                                        }).OrderByDescending(z => z.tno).Union(
                                            y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 => new
                                            {
                                                i1.odds,
                                                i1.otype,
                                                i1.oname,
                                                i1.tno,
                                                size = i1.odds == 0 ? 0 : i1.size
                                            }).OrderBy(z => z.tno)))
                                    }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                                }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData1" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }
                        if (masterdata.t1.FirstOrDefault().iplay == false)
                        {
                            var data = gdata.Where(p => p.company.Any(i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true
                            && Convert.ToBoolean(p.boplay) == true).Select(l =>
                            new
                            {
                                mid = l.mid,
                                mname = l.mname,
                                status = l.status,
                                rc = l.rc,
                                sno = l.sno,
                                ocnt = l.ocnt,
                                gscode = l.gscode,
                                dtype = l.dtype,
                                gtype = l.gtype.ToString().ToLower(),
                                maxb = l.maxb,
                                max = l.max,
                                min = l.min,
                                rem = l.rem,
                                umaxbof = l.umaxbof,
                                l.biplay,
                                l.boplay,
                                iplay = masterdata.t1.FirstOrDefault().iplay,
                                section = l.section.Select(y =>
                                new
                                {
                                    sid = y.sid,
                                    sno = y.sno,
                                    gstatus = y.gstatus,
                                    nat = y.nat,
                                    psrno = y.psrno,
                                    gscode = y.gscode,
                                    y.max,
                                    y.min,
                                    y.rem,
                                    odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i =>
                                    new { i.odds, i.otype, i.oname, i.tno, size = i.odds == 0 ? 0 : i.size }).OrderByDescending(z => z.tno).Union(y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 =>
                                    new { i1.odds, i1.otype, i1.oname, i1.tno, size = i1.odds == 0 ? 0 : i1.size }).OrderBy(z => z.tno)))
                                }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                            }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData11" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }
                    }
                }
                if (gameData.m == 1)
                {
                    var masterdata = await _cacheManagerFoot.Getasync<GameMaster>(gameData.gmid.ToString(), gameData.m);
                    var gdata = await _cacheManagerFoot.GetAllkeysData<GameDataw>(gameData.gmid.ToString(), gameData.m);
                    if (gdata != null && gdata.Count() > 0)
                    {
                        if (masterdata.t1.FirstOrDefault().iplay == true)
                        {
                            var data = gdata.Where(p => p.company.Any(
                                i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true && Convert.ToBoolean(p.biplay) == true
                                ).Select(l => new
                                {
                                    mid = l.mid,
                                    mname = l.mname,
                                    status = l.status,
                                    rc = l.rc,
                                    sno = l.sno,
                                    ocnt = l.ocnt,
                                    gscode = l.gscode,
                                    dtype = l.dtype,
                                    gtype = l.gtype.ToString().ToLower(),
                                    maxb = l.maxb,
                                    max = l.max,
                                    min = l.min,
                                    rem = l.rem,
                                    umaxbof = l.umaxbof,
                                    l.biplay,
                                    l.boplay,
                                    iplay = masterdata.t1.FirstOrDefault().iplay,
                                    section = l.section.Select(y => new
                                    {
                                        sid = y.sid,
                                        sno = y.sno,
                                        gstatus = y.gstatus,
                                        nat = y.nat,
                                        psrno = y.psrno,
                                        gscode = y.gscode,
                                        y.max,
                                        y.min,
                                        y.rem,
                                        odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i => new
                                        {
                                            i.odds,
                                            i.otype,
                                            i.oname,
                                            i.tno,
                                            size = i.odds == 0 ? 0 : i.size
                                        }).OrderByDescending(z => z.tno).Union(
                                            y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 => new
                                            {
                                                i1.odds,
                                                i1.otype,
                                                i1.oname,
                                                i1.tno,
                                                size = i1.odds == 0 ? 0 : i1.size
                                            }).OrderBy(z => z.tno)))
                                    }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                                }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData1" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }
                        if (masterdata.t1.FirstOrDefault().iplay == false)
                        {
                            var data = gdata.Where(p => p.company.Any(i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true
                            && Convert.ToBoolean(p.boplay) == true).Select(l =>
                            new
                            {
                                mid = l.mid,
                                mname = l.mname,
                                status = l.status,
                                rc = l.rc,
                                sno = l.sno,
                                ocnt = l.ocnt,
                                gscode = l.gscode,
                                dtype = l.dtype,
                                gtype = l.gtype.ToString().ToLower(),
                                maxb = l.maxb,
                                max = l.max,
                                min = l.min,
                                rem = l.rem,
                                umaxbof = l.umaxbof,
                                l.biplay,
                                l.boplay,
                                iplay = masterdata.t1.FirstOrDefault().iplay,
                                section = l.section.Select(y =>
                                new
                                {
                                    sid = y.sid,
                                    sno = y.sno,
                                    gstatus = y.gstatus,
                                    nat = y.nat,
                                    psrno = y.psrno,
                                    gscode = y.gscode,
                                    y.max,
                                    y.min,
                                    y.rem,
                                    odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i =>
                                    new { i.odds, i.otype, i.oname, i.tno, size = i.odds == 0 ? 0 : i.size }).OrderByDescending(z => z.tno).Union(y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 =>
                                    new { i1.odds, i1.otype, i1.oname, i1.tno, size = i1.odds == 0 ? 0 : i1.size }).OrderBy(z => z.tno)))
                                }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                            }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData11" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }

                    }
                }
                if (gameData.m == 2)
                {
                    var masterdata = await _cacheManagerTenni.Getasync<GameMaster>(gameData.gmid.ToString(), gameData.m);
                    var gdata = await _cacheManagerTenni.GetAllkeysData<GameDataw>(gameData.gmid.ToString(), gameData.m);
                    if (gdata != null && gdata.Count() > 0)
                    {
                        if (masterdata.t1.FirstOrDefault().iplay == true)
                        {
                            var data = gdata.Where(p => p.company.Any(
                                i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true && Convert.ToBoolean(p.biplay) == true
                                ).Select(l => new
                                {
                                    mid = l.mid,
                                    mname = l.mname,
                                    status = l.status,
                                    rc = l.rc,
                                    sno = l.sno,
                                    ocnt = l.ocnt,
                                    gscode = l.gscode,
                                    dtype = l.dtype,
                                    gtype = l.gtype.ToString().ToLower(),
                                    maxb = l.maxb,
                                    max = l.max,
                                    min = l.min,
                                    rem = l.rem,
                                    umaxbof = l.umaxbof,
                                    l.biplay,
                                    l.boplay,
                                    iplay = masterdata.t1.FirstOrDefault().iplay,
                                    section = l.section.Select(y => new
                                    {
                                        sid = y.sid,
                                        sno = y.sno,
                                        gstatus = y.gstatus,
                                        nat = y.nat,
                                        psrno = y.psrno,
                                        gscode = y.gscode,
                                        y.max,
                                        y.min,
                                        y.rem,
                                        odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i => new
                                        {
                                            i.odds,
                                            i.otype,
                                            i.oname,
                                            i.tno,
                                            size = i.odds == 0 ? 0 : i.size
                                        }).OrderByDescending(z => z.tno).Union(
                                            y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 => new
                                            {
                                                i1.odds,
                                                i1.otype,
                                                i1.oname,
                                                i1.tno,
                                                size = i1.odds == 0 ? 0 : i1.size
                                            }).OrderBy(z => z.tno)))
                                    }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                                }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData1" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }
                        if (masterdata.t1.FirstOrDefault().iplay == false)
                        {
                            var data = gdata.Where(p => p.company.Any(i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true
                            && Convert.ToBoolean(p.boplay) == true).Select(l =>
                            new
                            {
                                mid = l.mid,
                                mname = l.mname,
                                status = l.status,
                                rc = l.rc,
                                sno = l.sno,
                                ocnt = l.ocnt,
                                gscode = l.gscode,
                                dtype = l.dtype,
                                gtype = l.gtype.ToString().ToLower(),
                                maxb = l.maxb,
                                max = l.max,
                                min = l.min,
                                rem = l.rem,
                                umaxbof = l.umaxbof,
                                l.biplay,
                                l.boplay,
                                iplay = masterdata.t1.FirstOrDefault().iplay,
                                section = l.section.Select(y =>
                                new
                                {
                                    sid = y.sid,
                                    sno = y.sno,
                                    gstatus = y.gstatus,
                                    nat = y.nat,
                                    psrno = y.psrno,
                                    gscode = y.gscode,
                                    y.max,
                                    y.min,
                                    y.rem,
                                    odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i =>
                                    new { i.odds, i.otype, i.oname, i.tno, size = i.odds == 0 ? 0 : i.size }).OrderByDescending(z => z.tno).Union(y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 =>
                                    new { i1.odds, i1.otype, i1.oname, i1.tno, size = i1.odds == 0 ? 0 : i1.size }).OrderBy(z => z.tno)))
                                }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                            }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData11" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }

                    }
                }
                if (gameData.m == 3)
                {
                    var masterdata = await _cacheManagerOther.Getasync<GameMaster>(gameData.gmid.ToString(), gameData.m);
                    var gdata = await _cacheManagerOther.GetAllkeysData<GameDataw>(gameData.gmid.ToString(), gameData.m);
                    if (gdata != null && gdata.Count() > 0)
                    {
                        if (masterdata.t1.FirstOrDefault().iplay == true)
                        {
                            var data = gdata.Where(p => p.company.Any(
                                i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true && Convert.ToBoolean(p.biplay) == true
                                ).Select(l => new
                                {
                                    mid = l.mid,
                                    mname = l.mname,
                                    status = l.status,
                                    rc = l.rc,
                                    sno = l.sno,
                                    ocnt = l.ocnt,
                                    gscode = l.gscode,
                                    dtype = l.dtype,
                                    gtype = l.gtype.ToString().ToLower(),
                                    maxb = l.maxb,
                                    max = l.max,
                                    min = l.min,
                                    rem = l.rem,
                                    umaxbof = l.umaxbof,
                                    l.biplay,
                                    l.boplay,
                                    l.btcnt,
                                    iplay = masterdata.t1.FirstOrDefault().iplay,
                                    section = l.section.Select(y => new
                                    {
                                        sid = y.sid,
                                        sno = y.sno,
                                        gstatus = y.gstatus,
                                        nat = y.nat,
                                        psrno = y.psrno,
                                        gscode = y.gscode,
                                        y.max,
                                        y.min,
                                        y.rem,
                                        odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i => new
                                        {
                                            i.odds,
                                            i.otype,
                                            i.oname,
                                            i.tno,
                                            size = i.odds == 0 ? 0 : i.size
                                        }).OrderByDescending(z => z.tno).Union(
                                            y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 => new
                                            {
                                                i1.odds,
                                                i1.otype,
                                                i1.oname,
                                                i1.tno,
                                                size = i1.odds == 0 ? 0 : i1.size
                                            }).OrderBy(z => z.tno)))
                                    }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                                }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData1" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }
                        if (masterdata.t1.FirstOrDefault().iplay == false)
                        {
                            var data = gdata.Where(p => p.company.Any(i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.visible) == true
                            && Convert.ToBoolean(p.boplay) == true).Select(l =>
                            new
                            {
                                mid = l.mid,
                                mname = l.mname,
                                status = l.status,
                                rc = l.rc,
                                sno = l.sno,
                                ocnt = l.ocnt,
                                gscode = l.gscode,
                                dtype = l.dtype,
                                gtype = l.gtype.ToString().ToLower(),
                                maxb = l.maxb,
                                max = l.max,
                                min = l.min,
                                rem = l.rem,
                                umaxbof = l.umaxbof,
                                l.biplay,
                                l.boplay,
                                l.btcnt,
                                iplay = masterdata.t1.FirstOrDefault().iplay,
                                section = l.section.Select(y =>
                                new
                                {
                                    sid = y.sid,
                                    sno = y.sno,
                                    gstatus = y.gstatus,
                                    nat = y.nat,
                                    psrno = y.psrno,
                                    gscode = y.gscode,
                                    y.max,
                                    y.min,
                                    y.rem,
                                    odds = (y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "back").Select(i =>
                                    new { i.odds, i.otype, i.oname, i.tno, size = i.odds == 0 ? 0 : i.size }).OrderByDescending(z => z.tno).Union(y.odds.Where(a => a.sid == y.sid && a.otype.ToLower() == "lay").Select(i1 =>
                                    new { i1.odds, i1.otype, i1.oname, i1.tno, size = i1.odds == 0 ? 0 : i1.size }).OrderBy(z => z.tno)))
                                }).OrderBy(h => h.sno).ThenBy(a => a.sid)
                            }).OrderBy(x => x.sno).ToList();
                            //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            //ErrorLog.WriteLog("GameData11" + s);
                            if (data != null && data.Count() > 0)
                                return Return200("Success", data);
                            return Return300("No record found.");
                        }

                    }
                }
                return Return300("No record found.");
                //}
                //else
                //{
                //    return Return100("");
                //}

            }
            catch (Exception ex)
            {
                //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                ErrorLog.WriteLog("GameData", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(gameData));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("horsedetail")]
        public HttpResponseMessage Horsedetail([FromBody] Horsedetail paymentval)
        {
            try
            {
                ErrorLog.WriteLogAll("Horsedetail", JsonConvert.SerializeObject(paymentval));

                //string u_id = string.Empty;
                //var header = HttpContext.Current.Request.Headers["Authorization"].ToString();
                //if (header != null)
                //{
                //    u_id = JwtTokenGenerator.GetSaGuid(header);
                //}
                //Guid uu_id;
                //bool res = Guid.TryParse(u_id, out uu_id);

                //if (!res)
                //    return Return300("token not valid.");

                var Response = _DimFrontDiamondservice.Horsedetail(paymentval);

                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("Horsedetail", JsonConvert.SerializeObject(paymentval), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                var t1 = Response.Tables[0].Rows.Count <= 0 ? null : Response.Tables[0].AsEnumerable()
                   .Select(x => new
                   {//[sectionid],[runnername],[srno],[jockyname],[trainername],[horseage],[horseimage]
                       sid = string.IsNullOrEmpty(x["sectionid"].ToString()) ? 0 : x["sectionid"],
                       rname = string.IsNullOrEmpty(x["runnername"].ToString()) ? "" : x["runnername"].ToString().ToLower(),
                       sno = string.IsNullOrEmpty(x["srno"].ToString()) ? 0 : x["srno"],
                       jname = string.IsNullOrEmpty(x["jockyname"].ToString()) ? "" : x["jockyname"],
                       tname = string.IsNullOrEmpty(x["trainername"].ToString()) ? "" : x["trainername"],
                       hage = string.IsNullOrEmpty(x["horseage"].ToString()) ? 0 : x["horseage"],
                       himg = string.IsNullOrEmpty(x["horseimage"].ToString()) ? "" : x["horseimage"]
                   }).ToList();

                return Return200("success", t1);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Horsedetail : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(paymentval));
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("placebetdcongk")]
        public object PlacebetDconGK([FromBody] PlacebetDconGK placebetteen)
        {
            try
            {
                ErrorLog.WriteLogAll("PlacebetDconGK", JsonConvert.SerializeObject(placebetteen));

                placebetteen.btype = placebetteen.btype.ToLower();
                var Response = _DimFrontDiamondservice.PlacebetDconGK(placebetteen);

                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("PlacebetDconGK", JsonConvert.SerializeObject(placebetteen), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());

                //var t1 = new
                //{//select 1 as ID,'OK' as MSG,1 as automanual,@betdelay as betdelay, @amount as conformamount,@oddsec as oddsec,@NewUserrate as newuserrate
                //    id = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["ID"].ToString()) ? 0 : Response.Tables[0].Rows[0]["ID"],
                //    msg = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["MSG"].ToString()) ? "" : Response.Tables[0].Rows[0]["MSG"].ToString(),
                //    automan = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["automanual"].ToString()) ? 0 : Response.Tables[0].Rows[0]["automanual"],
                //    bdelay = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["betdelay"].ToString()) ? 0 : Response.Tables[0].Rows[0]["betdelay"],
                //    conamt = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["conformamount"].ToString()) ? 0 : Response.Tables[0].Rows[0]["conformamount"],
                //    osec = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["oddsec"].ToString()) ? "" : Response.Tables[0].Rows[0]["oddsec"].ToString(),
                //    newurate = string.IsNullOrEmpty(Response.Tables[0].Rows[0]["newuserrate"].ToString()) ? 0 : Response.Tables[0].Rows[0]["newuserrate"],
                //};

                return Return200("success", Response);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("PlacebetDconGK : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(placebetteen));
                return Return400("Server Error");
            }
        }
        public static string EncryptString(string key, string plainText)
        {
            byte[] iv = new byte[16];
            byte[] array;

            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(key);
                aes.IV = iv;

                ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV);

                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter streamWriter = new StreamWriter(cryptoStream))
                        {
                            streamWriter.Write(plainText);
                        }
                        array = memoryStream.ToArray();
                    }
                }
            }
            return Base64UrlEncoder.Encode(array);
        }
        #endregion
    }
    public static class StringCompression
    {
        /// <summary>
        /// Compresses a string and returns a deflate compressed, Base64 encoded string.
        /// </summary>
        /// <param name="uncompressedString">String to compress</param>
        public static string Compress(this string uncompressedString)
        {
            byte[] compressedBytes;

            using (var uncompressedStream = new MemoryStream(Encoding.UTF8.GetBytes(uncompressedString)))
            {
                using (var compressedStream = new MemoryStream())
                {
                    // setting the leaveOpen parameter to true to ensure that compressedStream will not be closed when compressorStream is disposed
                    // this allows compressorStream to close and flush its buffers to compressedStream and guarantees that compressedStream.ToArray() can be called afterward
                    // although MSDN documentation states that ToArray() can be called on a closed MemoryStream, I don't want to rely on that very odd behavior should it ever change
                    using (var compressorStream = new DeflateStream(compressedStream, CompressionLevel.Fastest, true))
                    {
                        uncompressedStream.CopyTo(compressorStream);
                    }

                    // call compressedStream.ToArray() after the enclosing DeflateStream has closed and flushed its buffer to compressedStream
                    compressedBytes = compressedStream.ToArray();
                }
            }

            return Convert.ToBase64String(compressedBytes);
        }

        /// <summary>
        /// Decompresses a deflate compressed, Base64 encoded string and returns an uncompressed string.
        /// </summary>
        /// <param name="compressedString">String to decompress.</param>
        public static string Decompress(this string compressedString)
        {
            byte[] decompressedBytes;

            var compressedStream = new MemoryStream(Convert.FromBase64String(compressedString));

            using (var decompressorStream = new DeflateStream(compressedStream, CompressionMode.Decompress))
            {
                using (var decompressedStream = new MemoryStream())
                {
                    decompressorStream.CopyTo(decompressedStream);

                    decompressedBytes = decompressedStream.ToArray();
                }
            }

            return Encoding.UTF8.GetString(decompressedBytes);
        }
        public static string RemoveInvalidChars(this string strSource)
        {
            return Regex.Replace(strSource, @"[^0-9a-zA-Z=+\/]", "");
        }
    }
    public static class CompressionExtensions
    {
        public static IEnumerable<byte> Zip(this object obj)
        {
            byte[] bytes = obj.Serialize();

            using (MemoryStream msi = new MemoryStream(bytes))
            using (MemoryStream mso = new MemoryStream())
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress))
                    msi.CopyToAsync(gs);

                return mso.ToArray().AsEnumerable();
            }
        }

        public static Task<object> Unzip(this byte[] bytes)
        {
            using (MemoryStream msi = new MemoryStream(bytes))
            using (MemoryStream mso = new MemoryStream())
            {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    // Sync example:
                    //gs.CopyTo(mso);

                    // Async way (take care of using async keyword on the method definition)
                    gs.CopyToAsync(mso);
                }

                return mso.ToArray().Deserialize();
            }
        }
    }

    public static class SerializerExtensions
    {
        public static byte[] Serialize<T>(this T objectToWrite)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                binaryFormatter.Serialize(stream, objectToWrite);

                return stream.GetBuffer();
            }
        }

        public static async Task<T> _Deserialize<T>(this byte[] arr)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                await stream.WriteAsync(arr, 0, arr.Length);
                stream.Position = 0;

                return (T)binaryFormatter.Deserialize(stream);
            }
        }

        public static async Task<object> Deserialize(this byte[] arr)
        {
            object obj = await arr._Deserialize<object>();
            return obj;
        }
    }

}
