using Common;
using Data;
using Models.DimFront;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;

namespace Services.DimFrontOpenser
{
    public class DimFrontService : IDimFrontService
    {
        #region Fields

        private readonly ISqlClientService _sqlClientService;

        #endregion

        #region Ctor

        public DimFrontService(ISqlClientService sqlClientService)
        {
            _sqlClientService = sqlClientService;
        }

        #endregion

        #region Methods
        public DataSet Framelogin(Framelogin framelogin)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = framelogin.gtype });
            parameters.Add(new SqlParameter() { ParameterName = "tokensession", Value = framelogin.token });
            parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "UserAuth" });
            DataSet ds = _sqlClientService.Execute("checkFramelogin", ConfigItems.Conn_AccD, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Tvata(Tvata tvata)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = tvata.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "mobiletype", Value = tvata.mtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getgametv" });
                ds = _sqlClientService.Execute("tv_userdata", ConfigItems.Conn_Odds, parameters);
                //_errorLogService.WriteLog("Tvata+tv_userdata", JsonConvert.SerializeObject(tvata) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Usercreate(Usercreate userMaster)
        {
            try
            {

                DataSet ds = new DataSet();
                DataSet ds1 = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UserName", Value = userMaster.uname });
                parameters.Add(new SqlParameter() { ParameterName = "Fullname", Value = userMaster.fname });
                parameters.Add(new SqlParameter() { ParameterName = "Password", Value = userMaster.pass });
                parameters.Add(new SqlParameter() { ParameterName = "City", Value = userMaster.ct });
                parameters.Add(new SqlParameter() { ParameterName = "MobileNo", Value = userMaster.mno });
                parameters.Add(new SqlParameter() { ParameterName = "RefCode", Value = userMaster.refc });
                parameters.Add(new SqlParameter() { ParameterName = "webdomain1", Value = userMaster.webdom });
                ds = _sqlClientService.Execute("WolfUserCreation", ConfigItems.Conn_AccD, parameters);
                //_errorLogService.WriteLog("Usercreate+WolfUserCreation", JsonConvert.SerializeObject(userMaster) + "Res:" + JsonConvert.SerializeObject(ds));
                try
                {
                    if (ds != null && ds.Tables != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Columns.Contains("id"))
                    {
                        var parameters1 = new List<SqlParameter>();
                        parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = ds.Tables[0].Rows[0]["userid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "parentid", Value = ds.Tables[0].Rows[0]["parentid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "UGuid", Value = ds.Tables[0].Rows[0]["UGuid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "username", Value = ds.Tables[0].Rows[0]["username"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "password", Value = ds.Tables[0].Rows[0]["password"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "fullname", Value = ds.Tables[0].Rows[0]["fullname"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "mobileno", Value = ds.Tables[0].Rows[0]["mobileno"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "city", Value = ds.Tables[0].Rows[0]["city"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "subid", Value = ds.Tables[0].Rows[0]["subid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "creditamt", Value = ds.Tables[0].Rows[0]["creditamt"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "webref", Value = ds.Tables[0].Rows[0]["webref"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "webdomain", Value = ds.Tables[0].Rows[0]["webdomain"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "levelno", Value = ds.Tables[0].Rows[0]["levelno"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = ds.Tables[0].Rows[0]["partnershiptype"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "panelpart", Value = ds.Tables[0].Rows[0]["panelpart"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "currate", Value = ds.Tables[0].Rows[0]["currate"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "partnership", Value = ds.Tables[0].Rows[0]["partnership"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "SaUserId", Value = ds.Tables[0].Rows[0]["SaUserId"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "SAGuid", Value = ds.Tables[0].Rows[0]["SAGuid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "IsMainAccount", Value = ds.Tables[0].Rows[0]["IsMainAccount"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "Permission", Value = ds.Tables[0].Rows[0]["Permission"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "issucessfull", Value = ds.Tables[0].Rows[0]["issucessfull"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ProcedureName", Value = ds.Tables[0].Rows[0]["ProcedureName"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "statement1", Value = ds.Tables[0].Rows[0]["statement"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "message", Value = ds.Tables[0].Rows[0]["message"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ipaddress", Value = userMaster.ip });
                        parameters1.Add(new SqlParameter() { ParameterName = "browserdetail", Value = userMaster.bdetail });
                        parameters1.Add(new SqlParameter() { ParameterName = "Statement", Value = "" });
                        ds1 = _sqlClientService.Execute("InsertUserLog", ConfigItems.Conn_LogD, parameters1);
                        //_errorLogService.WriteLog("Usercreate+InsertUserLog", JsonConvert.SerializeObject(userMaster) + "Res:" + JsonConvert.SerializeObject(ds1));
                    }
                }
                catch (Exception ex)
                {
                    //_errorLogService.WriteLogSe("Usercreate+WolfUserCreation", JsonConvert.SerializeObject(userMaster) + "Res:" + JsonConvert.SerializeObject(ds), ex.ToString());
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet UsercreateData(string json)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Json", Value = json });
                DataSet ds = _sqlClientService.Execute("InsertNewUserData", ConfigItems.Conn_Casino, parameters);
                //_errorLogService.WriteLog("UsercreateData+InsertNewUserData", json + "Res:" + JsonConvert.SerializeObject(ds));
                if (ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Json", Value = json });
                    _sqlClientService.Execute("InsertNewUserData", ConfigItems.Conn_Bet, parameters1);
                    //_errorLogService.WriteLog("UsercreateData+InsertNewUserData1", json + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;

                }
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Userexist(Userexist tvata)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UName", Value = tvata.uname });
                ds = _sqlClientService.Execute("WolfUserExist", ConfigItems.Conn_AccD, parameters);
                WriteLogAll("Userexist+WolfUserExist", JsonConvert.SerializeObject(tvata) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public static void WriteLogAll(String str, String request = null)
        {
            try
            {
                if (ConfigItems.AllLog)
                {
                    string path = System.Web.Hosting.HostingEnvironment.MapPath("~/Log/Service/Log_" + DateTime.Now.ToString("dd_MMM_yyyy") + ".txt");
                    if (!File.Exists(path))
                    {
                        var myFile = File.Create(path);
                        myFile.Close();
                    }
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine("---------------------------------------------------------------------------------------------");
                        //sw.Write(DateTime.Now);.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                        sw.Write(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture));
                        sw.WriteLine(Environment.NewLine + str + Environment.NewLine + request);
                    }
                }
            }
            catch (Exception)
            { }
        }
        #endregion
    }
}
