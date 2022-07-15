using Common;
using DimFrontOPenserGroup.AutherizationAtteributes;
using DimFrontOPenserGroup.Providers;
using ELog;
using Microsoft.Ajax.Utilities;
using Models.DimFrontGroup;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Services.DataPgGroup;
using Services.DimFrontOpenserGroup;
using Services.RedisManagerOpen;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;

namespace DimFrontOPenserGroup.Controllers
{
    [ModelValid]
    [RoutePrefix("api/dimfront")]
    public class FrontopenGroupController : ApiController
    {
        #region Fields

        private readonly IDimFrontOPenGroupService _DimFrontOPenserGroupservice;
        private readonly INpgadminGroupService _npgadminService;
        private readonly IRedis _cache;
        #endregion

        #region Ctor

        public FrontopenGroupController(IDimFrontOPenGroupService DimFrontOPenserGroupservice, IRedis cache, INpgadminGroupService npgadminService)
        {
            _DimFrontOPenserGroupservice = DimFrontOPenserGroupservice;
            _npgadminService = npgadminService;
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
        [HttpGet]
        [Route("~/test")]
        public HttpResponseMessage Test()
        {
            //var v = Assembly.GetExecutingAssembly().GetName().Version;
            return Return200("v:1.1");
        }
        [HttpPost]
        [Route("framelogintv")]
        public HttpResponseMessage Framelogintv([FromBody] Framelogin framelogin)
        {
            try
            {
                ErrorLog.WriteLogAll("Framelogintv", JsonConvert.SerializeObject(framelogin));
                var Response = _DimFrontOPenserGroupservice.Framelogin(framelogin);
                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("Framelogintv", JsonConvert.SerializeObject(framelogin), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                //select 1 as Id,'Completed sucessfully' as MSG,m.Userid,UserName,@idval1 as token,(CasinoBalance + SportsBalance + Balance) as General,'INR' as currency
                var t1 = new
                {
                    uname = Response.Tables[0].Rows[0]["UserName"].ToString(),
                    cur = Response.Tables[0].Rows[0]["currency"],
                    gen = Response.Tables[0].Rows[0]["General"],
                    msg = Response.Tables[0].Rows[0]["MSG"].ToString(),
                    token = Response.Tables[0].Rows[0]["token"]
                };
                return Return200("Success", t1);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Framelogintv", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(framelogin));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("tvdatawa")]
        public HttpResponseMessage TvdataWA([FromBody] Tvata tvata)
        {
            try
            {
                ErrorLog.WriteLogAll("TvdataWA", JsonConvert.SerializeObject(tvata));

                var Response = _DimFrontOPenserGroupservice.Tvata(tvata);

                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("TvdataWA", JsonConvert.SerializeObject(tvata), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
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
                    {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                        id = string.IsNullOrEmpty(x["ID"].ToString()) ? 0 : x["ID"],
                        ip = string.IsNullOrEmpty(x["Ipaddress"].ToString()) ? 0 : x["Ipaddress"],
                        pno = string.IsNullOrEmpty(x["portno"].ToString()) ? 0 : x["portno"],
                        cam = string.IsNullOrEmpty(x["Camstring"].ToString()) ? "" : x["Camstring"],
                        ssl = string.IsNullOrEmpty(x["sslflag"].ToString()) ? "" : x["sslflag"],
                        ptype = string.IsNullOrEmpty(x["porttype"].ToString()) ? "" : x["porttype"],
                        ttype = string.IsNullOrEmpty(x["tvtype"].ToString()) ? "" : x["tvtype"],
                    }).ToList();
                return Return200("success", t1);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("TvdataWA : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(tvata));
                return Return400("Server Error");
            }
        }
        //[HttpPost]
        //[Route("maintenance")]
        //public HttpResponseMessage Maintenance([FromBody] Maintenance maintenance)
        //{
        //    try
        //    {
        //        ErrorLog.WriteLogAll("Maintenance", JsonConvert.SerializeObject(maintenance));
        //        ErrorLog.Writemain(maintenance.ismain.ToString());
        //        if (maintenance.ismain)
        //        {
        //            _cache.Flush(ConfigItems.RedisLocaldb.ToString());
        //        }
        //        return Return200("Success");
        //    }
        //    catch (Exception ex)
        //    {
        //        ErrorLog.WriteLog("Maintenance : " + ex.ToString(), " : Req" + "");
        //        return Return400("Server Error");
        //    }
        //}
        [HttpPost]
        [Route("usercreate")]
        public HttpResponseMessage Usercreate([FromBody] Usercreate insertUser)
        {
            try
            {
                ErrorLog.WriteLogAll("Usercreate", JsonConvert.SerializeObject(insertUser));
                string u_id = string.Empty;

                var Response = _DimFrontOPenserGroupservice.Usercreate(insertUser);

                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("Usercreate", JsonConvert.SerializeObject(insertUser), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());

                if (Response.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    if (Response.Tables[0].Rows[0]["json"].ToString().Length > 1)
                    {
                        var Res = _DimFrontOPenserGroupservice.UsercreateData(Response.Tables[0].Rows[0]["json"].ToString());
                        if (Res != null && Response.Tables.Count > 0 && Res.Tables[Res.Tables.Count - 1].Columns.Contains("id") && Res.Tables[Res.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                        {
                            ErrorLog.WriteLog("UsercreateData", Res.Tables[0].Rows[0]["json"].ToString(), Res.Tables[Res.Tables.Count - 1].Rows[0]["MSG"].ToString());
                            return Return400("Data Error");
                        }
                        if (Res == null || Res.Tables.Count <= 0)
                            return Return300("No Data Found.");
                        if (Res.Tables[0].Rows.Count <= 0)
                            return Return300("No Record Found.");
                        if (Res.Tables[0].Columns.Contains("id") && Res.Tables[0].Rows[0]["id"].ToString() == "0")
                            return Return300(Res.Tables[0].Rows[0]["MSG"].ToString());
                        //return Return200(Res.Tables[0].Rows[0]["MSG"].ToString());
                    }
                    //return Return200(Res.Tables[0].Rows[0]["MSG"].ToString());
                }
                return Return200(Response.Tables[0].Rows[0]["MSG"].ToString());
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Usercreate" + ex.ToString(), " : Req" + JsonConvert.SerializeObject(insertUser));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("userexist")]
        public HttpResponseMessage Userexist([FromBody] Userexist insertUser)
        {
            try
            {
                ErrorLog.WriteLogAll("Userexist", JsonConvert.SerializeObject(insertUser));
                string u_id = string.Empty;

                var Response = _DimFrontOPenserGroupservice.Userexist(insertUser);

                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("Userexist", JsonConvert.SerializeObject(insertUser), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());

                return Return200(Response.Tables[0].Rows[0]["MSG"].ToString());
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Userexist" + ex.ToString(), " : Req" + JsonConvert.SerializeObject(insertUser));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("getsportteam")]
        public async Task<HttpResponseMessage> GetSportteam()
        {
            try
            {
                //return Return300("No record found");
                ErrorLog.WriteLogAll("GetSportteam", "");
                if (ConfigItems.isfixture == true)
                {
                    string data = "";
                    data = await _cache.GetSetMembersstr(GlobalCacheKey.Set, ConfigItems.Redisudb, ConfigItems.re);
                    data = data.RemoveInvalidChars();
                    var decompressedString = data.Decompress();
                    var da = JsonConvert.DeserializeObject(decompressedString);
                    if (data != null && data.Count() > 0)
                        return Return200("Success", da);
                }
                return Return300("No record found");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("GetSportteam : " + ex.ToString(), " : Req" + "");
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("gethighlightdata")]
        public async Task<HttpResponseMessage> Gethighlightdata(Gethighlightdata gethighlightdata)
        {
            try
            {
                //return Return300("No record found");
                ErrorLog.WriteLogAll("Gethighlightdata", JsonConvert.SerializeObject(gethighlightdata));

                string data = "";
                data = await _cache.GetSetMembersstr(gethighlightdata.key, ConfigItems.RedisLocaldb, ConfigItems.re);
                if (data != null)
                    return Return200("Success", JToken.Parse(data.ToString()));
                return Return300("No record found");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Gethighlightdata : " + ex.ToString(), " : Req" + "");
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("getotheralldata")]
        public async Task<HttpResponseMessage> GetOtheralldata()
        {
            try
            {
                //return Return300("No record found");
                ErrorLog.WriteLogAll("GetOtheralldata", "");
                string[] keys = "ourcasino,virtualcasino,fantasy,others,livecasino,sportlist".Split(',');
                string data = "";
                string data1 = "";
                string data2 = "";
                string data3 = "";
                string data4 = "";
                string data5 = "";

                data = await _cache.GetSetMembersstr(keys[0], ConfigItems.RedisLocaldb, ConfigItems.re);
                data1 = await _cache.GetSetMembersstr(keys[1], ConfigItems.RedisLocaldb, ConfigItems.re);
                data2 = await _cache.GetSetMembersstr(keys[2], ConfigItems.RedisLocaldb, ConfigItems.re);
                data3 = await _cache.GetSetMembersstr(keys[3], ConfigItems.RedisLocaldb, ConfigItems.re);
                data4 = await _cache.GetSetMembersstr(keys[4], ConfigItems.RedisLocaldb, ConfigItems.re);
                data5 = await _cache.GetSetMembersstr(keys[5], ConfigItems.RedisLocaldb, ConfigItems.re);

                return Return200("Success", new { oc = data != null ? JToken.Parse(data.ToString()) : "", vc = data1 != null ? JToken.Parse(data1.ToString()) : "", fan = data2 != null ? JToken.Parse(data2.ToString()) : "", ot = data3 != null ? JToken.Parse(data3.ToString()) : "", lc = data4 != null ? JToken.Parse(data4.ToString()) : "", sp = data5 != null ? JToken.Parse(data5.ToString()) : "" });
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("GetOtheralldata : " + ex.ToString(), " : Req" + "");
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("wolfpromotionreg")]
        public object Wolfpromotionreg([FromBody] Wolfpromotionreg sau)
        {
            try
            {
                ErrorLog.WriteLogAll("Wolfpromotionreg", JsonConvert.SerializeObject(sau));

                var resp = HttpHelper.Post(ConfigItems.Postgres + ApiEndpoint.postgres, JsonConvert.SerializeObject(sau), "application/json", "POST");
                //ErrorLog.WriteLogAll("RptDim1", JsonConvert.SerializeObject(resp));
                return JToken.Parse(resp);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Wolfpromotionreg : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(sau));
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("wolfpromotion2reg")]
        public object Wolfpromotion2reg([FromBody] Wolfpromotionreg sau)
        {
            try
            {
                ErrorLog.WriteLogAll("Wolfpromotion2reg", JsonConvert.SerializeObject(sau));

                var resp = HttpHelper.Post(ConfigItems.Postgres + ApiEndpoint.postgres1, JsonConvert.SerializeObject(sau), "application/json", "POST");
                //ErrorLog.WriteLogAll("RptDim1", JsonConvert.SerializeObject(resp));
                return JToken.Parse(resp);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Wolfpromotion2reg : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(sau));
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("wolfinquiriesreg")]
        public object Wolfinquiriesreg([FromBody] Wolfinquiriesreg sau)
        {
            try
            {
                ErrorLog.WriteLogAll("Wolfinquiriesreg", JsonConvert.SerializeObject(sau));

                var resp = HttpHelper.Post(ConfigItems.Postgres + ApiEndpoint.inquiries, JsonConvert.SerializeObject(sau), "application/json", "POST");
                //ErrorLog.WriteLogAll("RptDim1", JsonConvert.SerializeObject(resp));
                return JToken.Parse(resp);
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Wolfinquiriesreg : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(sau));
                return Return400("Server Error");
            }
        }
        //[HttpPost]
        //[Route("wolfinquiriesreg1")]
        //public object Wolfinquiriesreg1([FromBody] Wolfinquiriesreg sau)
        //{
        //    try
        //    {
        //        ErrorLog.WriteLogAll("Wolfinquiriesreg1", JsonConvert.SerializeObject(sau));

        //        var resp = HttpHelper.Post(ConfigItems.Postgres + ApiEndpoint.inquiries1, JsonConvert.SerializeObject(sau), "application/json", "POST");
        //        //ErrorLog.WriteLogAll("RptDim1", JsonConvert.SerializeObject(resp));
        //        return JToken.Parse(resp);
        //    }
        //    catch (Exception ex)
        //    {
        //        ErrorLog.WriteLog("Wolfinquiriesreg1 : " + ex.ToString(), " : Req" + JsonConvert.SerializeObject(sau));
        //        return Return400("Server Error");
        //    }
        //}
        [HttpPost]
        [Route("highlightdataopen")]
        public HttpResponseMessage HighlightDataopen(HighlightDataOpen highlightData)
        {
            try
            {
                ErrorLog.WriteLogAll("HighlightDataopen", JsonConvert.SerializeObject(highlightData));
                switch (highlightData.etid)
                {
                    case 4:
                        {
                            highlightData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 999:
                        {
                            highlightData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 1:
                        {
                            highlightData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablef"]);
                            break;
                        }
                    case 2:
                        {
                            highlightData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablet"]);
                            break;
                        }
                    default:
                        {
                            highlightData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tableo"]);
                            break;
                        }
                }
                string vid = highlightData.etid.ToString();
                if (highlightData.etid == 999)
                {
                    highlightData.etid = 4;
                }
                var Response = _npgadminService.HighlightDataopen(highlightData);
                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("HighlightDataopen", JsonConvert.SerializeObject(highlightData), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                var str = JsonConvert.DeserializeObject<HighlightData11>(Response.Tables[0].Rows[0]["value"].ToString());
                try
                {
                    if (str != null)
                    {
                        if (highlightData.action.ToLower() == "all")
                        {
                            var dataetid = str.t1.Where(_ => _.etid.ToString() == highlightData.etid.ToString());
                            if (dataetid != null && dataetid.Count() > 0)
                            {
                                var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                string keys = string.Join("','", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                var Response1 = _npgadminService.Hlgamede(keys, highlightData.tablename);
                                if (Response1 != null && Response1.Tables.Count > 0 && Response1.Tables[Response1.Tables.Count - 1].Columns.Contains("id") && Response1.Tables[Response1.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                                {
                                    ErrorLog.WriteLog("HighlightDataopen1", JsonConvert.SerializeObject(highlightData), Response1.Tables[Response1.Tables.Count - 1].Rows[0]["MSG"].ToString());
                                    return Return400("Data Error");
                                }
                                if (Response1 == null || Response1.Tables.Count <= 0)
                                    return Return300("No Data Found.");
                                if (Response1.Tables[0].Rows.Count <= 0)
                                    return Return300("No Record Found.");
                                if (Response1.Tables[0].Columns.Contains("id") && Response1.Tables[0].Rows[0]["id"].ToString() == "0")
                                    return Return300(Response1.Tables[0].Rows[0]["MSG"].ToString());
                                var mdata = Response1.Tables[0].Rows.Count <= 0 ? null : Response1.Tables[0].AsEnumerable()
                    .Select(x => new
                    {
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameMasterw>(x["value"].ToString()),
                    }).ToList();
                                string keysmid = string.Join("','", (dataetid1.AsEnumerable().Select(p => p.mid.ToString())));
                                var Response2 = _npgadminService.Hlgamedata(keysmid, highlightData.tablename);
                                var mdata1 = Response2 == null || Response2.Tables.Count <= 0 || Response2.Tables[0].Rows.Count <= 0 ? null : Response2.Tables[0].AsEnumerable()
                    .Select(x => new
                    {
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameDataw>(x["value"].ToString()),
                    }).ToList();

                                var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                               join rt in mdata1.Select(o => o.value)
                                                                               on pd.detail.gmid equals rt.gmid
                                                                               join rt1 in dataetid
                                                                               on new { pd.detail.gmid } equals new { rt1.gmid } into ji
                                                                               //from i in ji.DefaultIfEmpty()
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
                                                                                   //oid = i.oid,
                                                                                   gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                   section = rt == null ? null : rt.section.Where(h => h.mid == rt.mid).Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                               }).DistinctBy(w => w.mid).ToList() : null;
                                var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                                 join rt in dataetid2
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
                                                                                     mid = 0,
                                                                                     mname = "MATCH_ODDS",
                                                                                     status = "SUSPENDED",
                                                                                     rc = 2,
                                                                                     gscode = 0,
                                                                                     oid = rt.oid,
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
                                else if (highlightData.etid == 4 && vid == "4")
                                {
                                    var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                    var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                    if (t3.Count() == 0 && t4.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t3, t2 = t4 });
                                }
                                else if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
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
                                    if (query.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = query });
                                }
                                return Return200("Success", new { t1 = t1, t2 = t2 });
                            }
                        }
                        else if (highlightData.action.ToLower() == "today")
                        {
                            var dataetid = str.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.stime.ToString("MM/dd/yyyy") == DateTime.Now.ToString("MM/dd/yyyy"));
                            if (dataetid != null && dataetid.Count() > 0)
                            {
                                var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                string keys = string.Join("','", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                var Response1 = _npgadminService.Hlgamede(keys, highlightData.tablename);
                                if (Response1 != null && Response1.Tables.Count > 0 && Response1.Tables[Response1.Tables.Count - 1].Columns.Contains("id") && Response1.Tables[Response1.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                                {
                                    ErrorLog.WriteLog("HighlightDataopen1", JsonConvert.SerializeObject(highlightData), Response1.Tables[Response1.Tables.Count - 1].Rows[0]["MSG"].ToString());
                                    return Return400("Data Error");
                                }
                                if (Response1 == null || Response1.Tables.Count <= 0)
                                    return Return300("No Data Found.");
                                if (Response1.Tables[0].Rows.Count <= 0)
                                    return Return300("No Record Found.");
                                if (Response1.Tables[0].Columns.Contains("id") && Response1.Tables[0].Rows[0]["id"].ToString() == "0")
                                    return Return300(Response1.Tables[0].Rows[0]["MSG"].ToString());
                                var mdata = Response1.Tables[0].Rows.Count <= 0 ? null : Response1.Tables[0].AsEnumerable()
                    .Select(x => new
                    {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameMasterw>(x["value"].ToString()),
                    }).ToList();
                                string keysmid = string.Join("','", (dataetid1.AsEnumerable().Select(p => p.mid.ToString())));
                                var Response2 = _npgadminService.Hlgamedata(keysmid, highlightData.tablename);
                                var mdata1 = Response2 == null || Response2.Tables.Count <= 0 || Response2.Tables[0].Rows.Count <= 0 ? null : Response2.Tables[0].AsEnumerable()
                    .Select(x => new
                    {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameDataw>(x["value"].ToString()),
                    }).ToList();
                                var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                               join rt in mdata1.Select(o => o.value)
                                                                               on pd.detail.gmid equals rt.gmid
                                                                               join rt1 in dataetid
                                                                               on pd.detail.gmid equals rt1.gmid
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
                                                                                   oid = rt1.oid,
                                                                                   gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                   section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                               }).DistinctBy(w => w.mid).ToList() : null;
                                var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                                 join rt in dataetid2
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
                                                                                     mid = 0,
                                                                                     mname = "MATCH_ODDS",
                                                                                     status = "SUSPENDED",
                                                                                     rc = 2,
                                                                                     gscode = 0,
                                                                                     oid = rt.oid,
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
                                else if (highlightData.etid == 4 && vid == "4")
                                {
                                    var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                    var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                    if (t3.Count() == 0 && t4.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t3, t2 = t4 });
                                }
                                else if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
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
                                    if (query.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = query });
                                }
                                return Return200("Success", new { t1 = t1, t2 = t2 });
                            }
                        }
                        else if (highlightData.action.ToLower() == "inplay")
                        {
                            var dataetid = str.t1.Where(_ => _.etid == highlightData.etid.ToString() && _.iplay == true);
                            if (dataetid != null && dataetid.Count() > 0)
                            {
                                var dataetid1 = dataetid.Where(o => o.mid != "0").ToList();
                                var dataetid2 = dataetid.Where(o => o.mid == "0").ToList();
                                string keys = string.Join("','", (dataetid.AsEnumerable().Select(p => p.gmid.ToString())));
                                var Response1 = _npgadminService.Hlgamede(keys, highlightData.tablename);
                                if (Response1 != null && Response1.Tables.Count > 0 && Response1.Tables[Response1.Tables.Count - 1].Columns.Contains("id") && Response1.Tables[Response1.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                                {
                                    ErrorLog.WriteLog("HighlightDataopen1", JsonConvert.SerializeObject(highlightData), Response1.Tables[Response1.Tables.Count - 1].Rows[0]["MSG"].ToString());
                                    return Return400("Data Error");
                                }
                                if (Response1 == null || Response1.Tables.Count <= 0)
                                    return Return300("No Data Found.");
                                if (Response1.Tables[0].Rows.Count <= 0)
                                    return Return300("No Record Found.");
                                if (Response1.Tables[0].Columns.Contains("id") && Response1.Tables[0].Rows[0]["id"].ToString() == "0")
                                    return Return300(Response1.Tables[0].Rows[0]["MSG"].ToString());
                                var mdata = Response1.Tables[0].Rows.Count <= 0 ? null : Response1.Tables[0].AsEnumerable()
                    .Select(x => new
                    {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameMasterw>(x["value"].ToString()),
                    }).ToList();
                                string keysmid = string.Join("','", (dataetid1.AsEnumerable().Select(p => p.mid.ToString())));
                                var Response2 = _npgadminService.Hlgamedata(keysmid, highlightData.tablename);
                                var mdata1 = Response2 == null || Response2.Tables.Count <= 0 || Response2.Tables[0].Rows.Count <= 0 ? null : Response2.Tables[0].AsEnumerable()
                    .Select(x => new
                    {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                        id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                        value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameDataw>(x["value"].ToString()),
                    }).ToList();
                                var t1 = mdata != null && mdata.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                               join rt in mdata1.Select(o => o.value)
                                                                               on pd.detail.gmid equals rt.gmid
                                                                               join rt1 in dataetid
                                                                               on pd.detail.gmid equals rt1.gmid
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
                                                                                   oid = rt1.oid,
                                                                                   gtype = rt == null ? "" : rt.gtype.ToString().ToLower(),
                                                                                   section = rt == null ? null : rt.section.Select(y => new { sid = y.sid, sno = (y.sno == 2 ? 3 : (y.sno == 3 ? 2 : y.sno)), gstatus = y.gstatus, gscode = y.gscode, nat = y.nat, odds = y.odds.Where(z => z.tno == 0) }),
                                                                               }).DistinctBy(w => w.mid).ToList() : null;
                                var t2 = mdata1 != null && mdata1.Count() > 0 ? (from pd in mdata.Select(o => new { detail = o.value.t1.FirstOrDefault() })
                                                                                 join rt in dataetid2
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
                                                                                     mid = 0,
                                                                                     mname = "MATCH_ODDS",
                                                                                     status = "SUSPENDED",
                                                                                     rc = 2,
                                                                                     gscode = 0,
                                                                                     oid = rt.oid,
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
                                else if (highlightData.etid == 4 && vid == "4")
                                {
                                    var t3 = t1 != null ? t1.Where(s => s.iscc == 0).ToList() : null;
                                    var t4 = t2 != null ? t2.Where(s => s.iscc == 0).ToList() : null;
                                    if (t3.Count() == 0 && t4.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = t3, t2 = t4 });
                                }
                                else if (highlightData.etid.ToString() == "10" || highlightData.etid.ToString() == "12")
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
                                    if (query.Count() == 0)
                                        return Return300("No record found.");
                                    return Return200("Success", new { t1 = query });
                                }
                                return Return200("Success", new { t1 = t1, t2 = t2 });
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    ErrorLog.WriteLog("HighlightDataopen", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                    return Return300("invalid data.");
                }
                return Return300("No record found");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("HighlightDataopen", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(highlightData));
                return Return400(ex.Message);
            }

        }
        [HttpPost]
        [Route("gamedetailopen")]
        public HttpResponseMessage GameDetailOpen(GameDetailOpen gameDetail)
        {
            try
            {
                ErrorLog.WriteLogAll("GameDetailOpen", JsonConvert.SerializeObject(gameDetail));
                if (gameDetail.etid == 999)
                    gameDetail.etid = 4;
                switch (gameDetail.etid)
                {
                    case 4:
                        {
                            gameDetail.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 999:
                        {
                            gameDetail.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 1:
                        {
                            gameDetail.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablef"]);
                            break;
                        }
                    case 2:
                        {
                            gameDetail.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablet"]);
                            break;
                        }
                    default:
                        {
                            gameDetail.tablename = Convert.ToString(ConfigurationManager.AppSettings["tableo"]);
                            break;
                        }
                }
                var Response = _npgadminService.Gamedetail(gameDetail.gmid.ToString(), gameDetail.tablename);
                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("GameDetailOpen", JsonConvert.SerializeObject(gameDetail), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                var masterdata = JsonConvert.DeserializeObject<GameMaster>(Response.Tables[0].Rows[0]["value"].ToString());
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
                if (data != null)
                    return Return200("Success", data);
                return Return300("No Data Found.");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("GameDetailOpen", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(gameDetail));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("gamedataopen")]
        public HttpResponseMessage GameDataopen(GameDataopen gameData)
        {
            try
            {
                ErrorLog.WriteLogAll("GameDataopen", JsonConvert.SerializeObject(gameData));
                switch (gameData.etid)
                {
                    case 4:
                        {
                            gameData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 999:
                        {
                            gameData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablec"]);
                            break;
                        }
                    case 1:
                        {
                            gameData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablef"]);
                            break;
                        }
                    case 2:
                        {
                            gameData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tablet"]);
                            break;
                        }
                    default:
                        {
                            gameData.tablename = Convert.ToString(ConfigurationManager.AppSettings["tableo"]);
                            break;
                        }
                }
                var Response = _npgadminService.Gamedetail(gameData.gmid.ToString(), gameData.tablename);
                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("GameDetailOpen", JsonConvert.SerializeObject(gameData), Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                var masterdata = JsonConvert.DeserializeObject<GameMaster>(Response.Tables[0].Rows[0]["value"].ToString());
                var Response2 = _npgadminService.Gamedata(gameData.gmid.ToString(), gameData.tablename);
                var gdata = Response2 == null || Response2.Tables.Count <= 0 || Response2.Tables[0].Rows.Count <= 0 ? null : Response2.Tables[0].AsEnumerable()
                        .Select(x => new
                        {//top 1 1 as ID,@myip as Ipaddress,[portno],[Camstring],[sslflag],[porttype],[tvtype]
                            id = string.IsNullOrEmpty(x["key"].ToString()) ? "" : x["key"],
                            value = string.IsNullOrEmpty(x["value"].ToString()) ? null : JsonConvert.DeserializeObject<GameDataw>(x["value"].ToString()),
                        }).ToList();
                if (gdata != null && gdata.Count() > 0)
                {
                    if (masterdata.t1.FirstOrDefault().iplay == true)
                    {
                        var data = gdata.Where(p => p.value.company.Any(
                            i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.value.visible) == true && Convert.ToBoolean(p.value.biplay) == true
                            ).Select(l => new
                            {
                                mid = l.value.mid,
                                mname = l.value.mname,
                                status = l.value.status,
                                rc = l.value.rc,
                                sno = l.value.sno,
                                ocnt = l.value.ocnt,
                                gscode = l.value.gscode,
                                dtype = l.value.dtype,
                                gtype = l.value.gtype.ToString().ToLower(),
                                maxb = l.value.maxb,
                                max = l.value.max,
                                min = l.value.min,
                                rem = l.value.rem,
                                umaxbof = l.value.umaxbof,
                                l.value.biplay,
                                l.value.boplay,
                                iplay = masterdata.t1.FirstOrDefault().iplay,
                                section = l.value.section.Select(y => new
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
                        return Return200("Success", data);
                    }
                    if (masterdata.t1.FirstOrDefault().iplay == false)
                    {
                        var data = gdata.Where(p => p.value.company.Any(i => i.code == ConfigItems.CCode && Convert.ToBoolean(i.visible) == true) && Convert.ToBoolean(p.value.visible) == true
                        && Convert.ToBoolean(p.value.boplay) == true).Select(l =>
                        new
                        {
                            mid = l.value.mid,
                            mname = l.value.mname,
                            status = l.value.status,
                            rc = l.value.rc,
                            sno = l.value.sno,
                            ocnt = l.value.ocnt,
                            gscode = l.value.gscode,
                            dtype = l.value.dtype,
                            gtype = l.value.gtype.ToString().ToLower(),
                            maxb = l.value.maxb,
                            max = l.value.max,
                            min = l.value.min,
                            rem = l.value.rem,
                            umaxbof = l.value.umaxbof,
                            l.value.biplay,
                            l.value.boplay,
                            iplay = masterdata.t1.FirstOrDefault().iplay,
                            section = l.value.section.Select(y =>
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
                        return Return200("Success", data);
                    }

                }
                return Return300("No record found.");
            }
            catch (Exception ex)
            {
                //s += " | " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                ErrorLog.WriteLog("GameDataopen", ex.Message.ToString(), " : Req" + JsonConvert.SerializeObject(gameData));
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("treedataopen")]
        public HttpResponseMessage TreeDataopen()
        {
            try
            {
                ErrorLog.WriteLogAll("TreeDataopen", "");

                String tablename = Convert.ToString(ConfigurationManager.AppSettings["table"]); ;
                var Response = _npgadminService.Treedata(tablename);
                if (Response != null && Response.Tables.Count > 0 && Response.Tables[Response.Tables.Count - 1].Columns.Contains("id") && Response.Tables[Response.Tables.Count - 1].Rows[0]["id"].ToString() == "-1")
                {
                    ErrorLog.WriteLog("TreeDataopen", "", Response.Tables[Response.Tables.Count - 1].Rows[0]["MSG"].ToString());
                    return Return400("Data Error");
                }
                if (Response == null || Response.Tables.Count <= 0)
                    return Return300("No Data Found.");
                if (Response.Tables[0].Rows.Count <= 0)
                    return Return300("No Record Found.");
                if (Response.Tables[0].Columns.Contains("id") && Response.Tables[0].Rows[0]["id"].ToString() == "0")
                    return Return300(Response.Tables[0].Rows[0]["MSG"].ToString());
                var data = JsonConvert.DeserializeObject<TreeviewData>(Response.Tables[0].Rows[0]["value"].ToString());
                if (data != null)
                    return Return200("Success", data);
                else
                    return Return300("No record found.");
            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("TreeDataopen", ex.Message.ToString(), " : Req" + "");
                return Return400(ex.Message);
            }
        }
        [HttpPost]
        [Route("wolfprmotionpage")]
        public async Task<HttpResponseMessage> Wolfpromotionpage(Wolfpromotionpage wolfpromotionpage)
        {
            try
            {
                //return Return300("No record found");
                ErrorLog.WriteLogAll("Wolfpromotionpage", "");

                var datalst = await _cache.GetSetMembersstr("Casinolist", ConfigItems.RedisLocaldbclist, ConfigItems.re);

                //if (datalst != null)
                //{
                //    var datalst1 = JsonConvert.DeserializeObject<Casinolistwp>(datalst);
                //    var data = datalst1.t1.Where(_ => _.picon == 3 && _.gtype.ToLower() == "promotion").Select(o => new { o.clid, o.gmname, o.listono, o.nlunched, o.pid, gtype = o.gtype.ToLower(), o.m, o.picon, o.cname }).OrderBy(i => i.listono).ToList();
                //    if (data != null && data.Count() > 0)
                //        return Return200("Success", data);
                //}
                if (datalst != null)
                {
                    var datalst1 = JsonConvert.DeserializeObject<Casinolistwp>(datalst);
                    if (wolfpromotionpage.gtype.ToLower() == "promotion2")
                    {
                        var data = datalst1.t1.Where(_ => _.picon == 3 && _.gtype.ToLower() == wolfpromotionpage.gtype.ToLower()).Select(o => new { o.clid, o.gmname, o.listono, o.nlunched, o.pid, gtype = o.gtype.ToLower(), o.m, o.picon, o.cname }).OrderBy(i => i.listono).ToList();
                        if (data != null && data.Count() > 0)
                            return Return200("Success", data);
                    }
                    else
                    {
                        var data = datalst1.t1.Where(_ => _.picon == 3 && _.gtype.ToLower() == wolfpromotionpage.gtype.ToLower()).Select(o => new { o.clid, o.gmname, o.listono, o.nlunched, o.pid, gtype = o.gtype.ToLower(), o.m, o.picon, o.cname }).OrderBy(i => i.listono).ToList();
                        if (data != null && data.Count() > 0)
                            return Return200("Success", data);
                    }
                }
                return Return300("No record found.");

            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Wolfpromotionpage : " + ex.ToString(), " : Req" + "");
                return Return400("Server Error");
            }
        }
        [HttpPost]
        [Route("wolfproudpartners")]
        public async Task<HttpResponseMessage> Wolfproudpartners()
        {
            try
            {
                //return Return300("No record found");
                ErrorLog.WriteLogAll("Wolfproudpartners", "");

                var datalst = await _cache.GetSetMembersstr("Casinolist", ConfigItems.RedisLocaldbclist, ConfigItems.re);

                if (datalst != null)
                {
                    var datalst1 = JsonConvert.DeserializeObject<Casinolistwp>(datalst);
                    var data = datalst1.t1.Where(_ => _.picon == 3 && _.gtype.ToLower() == "proud partners").Select(o => new { o.clid, o.gmname, o.listono, o.nlunched, o.pid, gtype = o.gtype.ToLower(), o.m, o.picon, o.cname }).OrderBy(i => i.listono).ToList();
                    if (data != null && data.Count() > 0)
                        return Return200("Success", data);
                }
                return Return300("No record found.");

            }
            catch (Exception ex)
            {
                ErrorLog.WriteLog("Wolfproudpartners : " + ex.ToString(), " : Req" + "");
                return Return400("Server Error");
            }
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
