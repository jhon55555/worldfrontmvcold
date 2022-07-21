using Common;
using Data;
using Models.DimFrontGroup;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;

namespace Services.DimFrontGroup
{
    public class DimFrontGroupService : IDimFrontGroupService
    {
        #region Fields

        private readonly ISqlClientService _sqlClientService;

        #endregion

        #region Ctor

        public DimFrontGroupService(ISqlClientService sqlClientService)
        {
            _sqlClientService = sqlClientService;
        }

        #endregion

        #region Methods
        public DataSet RefreshToken(string userid, string rt)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "guid", Value = userid });
            parameters.Add(new SqlParameter() { ParameterName = "rt", Value = rt });
            return _sqlClientService.Execute("sp_refreshtoken", ConfigItems.Conn_LogD, parameters);
        }
        public DataSet UserLogin(UserLogin userLogin)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = userLogin.guid });
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = userLogin.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Userlogin" });
                DataSet ds = _sqlClientService.Execute("checkFramelogin", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("UserLogin", userLogin + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Framelogin(Framelogin framelogin)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = framelogin.gtype });
            parameters.Add(new SqlParameter() { ParameterName = "tokensession", Value = framelogin.token });
            parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "UserAuth" });
            DataSet ds = _sqlClientService.Execute("checkFramelogin", ConfigItems.Conn_AccDgroup, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet GetDate()
        {
            var parameters = new List<SqlParameter>();
            return _sqlClientService.Execute("SP_GetDate", ConfigItems.Conn_AccDgroup, parameters);
        }
        public DataSet ChkIP(String ip)
        {
            try
            {
                var param = new List<SqlParameter>();
                param.Add(new SqlParameter() { ParameterName = "Ip", Value = ip });
                return _sqlClientService.Execute("checkEndUserValidity", ConfigItems.Conn_LogD, param);
            }
            catch (Exception)
            {
                throw;
            }

        }
        public DataSet CheckWords(Guid uu_id, int uid, String ip, String param, String proname)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = uu_id });
                parameters.Add(new SqlParameter() { ParameterName = "Ip", Value = ip });
                parameters.Add(new SqlParameter() { ParameterName = "Perameters", Value = param });
                parameters.Add(new SqlParameter() { ParameterName = "ProcedreName", Value = proname });
                return _sqlClientService.Execute("InsertSqlInjection", ConfigItems.Conn_LogD, parameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Login(Login login)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UserName", Value = login.uname });
                parameters.Add(new SqlParameter() { ParameterName = "Password", Value = login.pass });
                parameters.Add(new SqlParameter() { ParameterName = "Webdomain", Value = login.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "Vpn", Value = login.vpn });
                parameters.Add(new SqlParameter() { ParameterName = "Host", Value = login.host });
                parameters.Add(new SqlParameter() { ParameterName = "ip", Value = login.ip });
                parameters.Add(new SqlParameter() { ParameterName = "browserdetail", Value = login.bdetail });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Login" });
                ds = _sqlClientService.Execute("EndUserLogin", ConfigItems.Conn_AccDgroup, parameters);
                try
                {
                    if (ds.Tables[0].Rows[0]["id"].ToString() == "1")
                    {
                        var parameters1 = new List<SqlParameter>();
                        parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = ds.Tables[0].Rows[0]["UserId"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "UGuid", Value = ds.Tables[0].Rows[0]["UGuid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "username", Value = login.uname });
                        parameters1.Add(new SqlParameter() { ParameterName = "UPassword", Value = login.pass });
                        parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "Login" });
                        parameters1.Add(new SqlParameter() { ParameterName = "MsgId", Value = ds.Tables[0].Rows[0]["id"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "message", Value = "success" });
                        parameters1.Add(new SqlParameter() { ParameterName = "ip", Value = login.ip });
                        //parameters1.Add(new SqlParameter() { ParameterName = "browserdetail", Value = login.bdetail });
                        parameters1.Add(new SqlParameter() { ParameterName = "ISSucessfull", Value = true });
                        parameters1.Add(new SqlParameter() { ParameterName = "CreatedBy", Value = ds.Tables[0].Rows[0]["UserId"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "Vpn", Value = login.host });
                        parameters1.Add(new SqlParameter() { ParameterName = "Host", Value = login.vpn });
                        parameters1.Add(new SqlParameter() { ParameterName = "Webdomain", Value = ds.Tables[0].Rows[0]["Webdomain"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "Webref", Value = ds.Tables[0].Rows[0]["Webref"] });
                        _sqlClientService.Execute("InsertEndUserLoginLog", ConfigItems.Conn_LogD, parameters1);

                        var parameters3 = new List<SqlParameter>();
                        parameters3.Add(new SqlParameter() { ParameterName = "userid", Value = ds.Tables[0].Rows[0]["UserId"] });
                        _sqlClientService.Execute("Logoutallsession", ConfigItems.Conn_CasinoTP, parameters3);
                    }
                    else
                    {
                        var parameters2 = new List<SqlParameter>();
                        parameters2.Add(new SqlParameter() { ParameterName = "userid", Value = 0 });
                        parameters2.Add(new SqlParameter() { ParameterName = "UGuid", Value = "00000000-0000-0000-0000-000000000000" });
                        parameters2.Add(new SqlParameter() { ParameterName = "username", Value = login.uname });
                        parameters2.Add(new SqlParameter() { ParameterName = "UPassword", Value = login.pass });
                        parameters2.Add(new SqlParameter() { ParameterName = "statement", Value = "Login" });
                        parameters2.Add(new SqlParameter() { ParameterName = "MsgId", Value = ds.Tables[0].Rows[0]["id"] });
                        parameters2.Add(new SqlParameter() { ParameterName = "message", Value = ds.Tables[0].Rows[0]["msg"].ToString() });
                        parameters2.Add(new SqlParameter() { ParameterName = "ip", Value = login.ip });
                        //parameters2.Add(new SqlParameter() { ParameterName = "browserdetail", Value = login.bdetail });
                        parameters2.Add(new SqlParameter() { ParameterName = "ISSucessfull", Value = false });
                        parameters2.Add(new SqlParameter() { ParameterName = "CreatedBy", Value = 0 });
                        parameters2.Add(new SqlParameter() { ParameterName = "Vpn", Value = login.host });
                        parameters2.Add(new SqlParameter() { ParameterName = "Host", Value = login.vpn });
                        parameters2.Add(new SqlParameter() { ParameterName = "Webdomain", Value = ds.Tables[0].Rows[0]["Webdomain"] });
                        parameters2.Add(new SqlParameter() { ParameterName = "Webref", Value = ds.Tables[0].Rows[0]["Webref"] });
                        _sqlClientService.Execute("InsertEndUserLoginLog", ConfigItems.Conn_LogD, parameters2);
                    }

                }
                catch (Exception ex)
                {
                    //_errorLogService.WriteLogSe("Login+EndUserLogin", JsonConvert.SerializeObject(login) + "Res:" + JsonConvert.SerializeObject(ds), ex.ToString());
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet VerifyCode(VerifyCode vc)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "tokenval", Value = vc.code });
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = vc.guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "codevalid" });
                DataSet ds = _sqlClientService.Execute("apk_service", ConfigItems.Conn_LogA, parameters);
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet GetAuth(Guid uguid, String lvlno)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Uu_id", Value = uguid });
                parameters.Add(new SqlParameter() { ParameterName = "levelno", Value = lvlno });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "selectuserauth" });
                return _sqlClientService.Execute("auth_twoway", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet AuthOn(Guid uguid, String lvlno)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Uu_id", Value = uguid });
                parameters.Add(new SqlParameter() { ParameterName = "levelno", Value = lvlno });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userdata" });
                DataSet ds = _sqlClientService.Execute("auth_twoway", ConfigItems.Conn_AccDgroup, parameters);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "username", Value = ds.Tables[1].Rows[0]["UserName"].ToString() });
                    parameters1.Add(new SqlParameter() { ParameterName = "userpass", Value = ds.Tables[1].Rows[0]["Password"].ToString() });
                    parameters1.Add(new SqlParameter() { ParameterName = "guid", Value = uguid });
                    parameters1.Add(new SqlParameter() { ParameterName = "levelno", Value = lvlno });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "insertuserback" });
                    return _sqlClientService.Execute("apk_service", ConfigItems.Conn_LogA, parameters1);
                }
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet AuthOff(VerifyCode vc)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "tokenval", Value = vc.code });
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = vc.guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "resetuservalid" });
                DataSet ds = _sqlClientService.Execute("apk_service", ConfigItems.Conn_LogA, parameters);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Uu_id", Value = vc.guid });
                    parameters1.Add(new SqlParameter() { ParameterName = "userauth", Value = "0" });
                    parameters1.Add(new SqlParameter() { ParameterName = "levelno", Value = vc.lvlno });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "userauthallow" });
                    DataSet ds1 = _sqlClientService.Execute("auth_twoway", ConfigItems.Conn_AccDgroup, parameters1);
                }
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet VerifyCodeTele(VerifyCode vc)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "tokenval", Value = vc.code });
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = vc.guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "codevalid" });
                DataSet ds = _sqlClientService.Execute("telegram_service", ConfigItems.Conn_LogA, parameters);
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet UserCheckTele(Guid uguid, String lvlno)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Uu_id", Value = uguid });
                parameters.Add(new SqlParameter() { ParameterName = "levelno", Value = lvlno });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userdatacheck" });
                DataSet ds = _sqlClientService.Execute("auth_tele", ConfigItems.Conn_AccDgroup, parameters);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "guid", Value = uguid });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "codegenerate" });
                    return _sqlClientService.Execute("telegram_service", ConfigItems.Conn_LogA, parameters1);
                }
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet GenOtpTele(Guid uguid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = uguid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "codegenerate" });
                return _sqlClientService.Execute("telegram_service", ConfigItems.Conn_LogA, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet AuthOnTele(AuthOnTele aot)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userpass", Value = aot.upass });
                parameters.Add(new SqlParameter() { ParameterName = "Uu_id", Value = aot.guid });
                parameters.Add(new SqlParameter() { ParameterName = "levelno", Value = aot.lvlno });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userdata" });
                DataSet ds = _sqlClientService.Execute("auth_tele", ConfigItems.Conn_AccDgroup, parameters);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "username", Value = ds.Tables[1].Rows[0]["UserName"].ToString() });
                    parameters1.Add(new SqlParameter() { ParameterName = "guid", Value = aot.guid });
                    parameters1.Add(new SqlParameter() { ParameterName = "levelno", Value = aot.lvlno });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "insertuserback" });
                    return _sqlClientService.Execute("telegram_service", ConfigItems.Conn_LogA, parameters1);
                }
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AuthOffTele(VerifyCode vc)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "tokenval", Value = vc.code });
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = vc.guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "removeuser" });
                DataSet ds = _sqlClientService.Execute("telegram_service", ConfigItems.Conn_LogA, parameters);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Uu_id", Value = vc.guid });
                    parameters1.Add(new SqlParameter() { ParameterName = "userauth", Value = "0" });
                    parameters1.Add(new SqlParameter() { ParameterName = "levelno", Value = vc.lvlno });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "userauthallow" });
                    DataSet ds1 = _sqlClientService.Execute("auth_tele", ConfigItems.Conn_AccDgroup, parameters1);
                }
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet BannerData()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "getBannerData" });
                return _sqlClientService.Execute("getBannerData", ConfigItems.Conn_Casinogroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet Casinolist(Casinolist casinolist)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "pageicon", Value = casinolist.picon });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "CasinoList" });
                DataSet ds = _sqlClientService.Execute("getBannerData", ConfigItems.Conn_Casinogroup, parameters);
                //_errorLogService.WriteLog("Casinolist", JsonConvert.SerializeObject(casinolist) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet TreeviewData()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Treeview" });
                return _sqlClientService.Execute("Dim_GameTreeView", ConfigItems.Conn_Odds, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet HighlightData(HighlightData highlightData)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "EventId", Value = highlightData.etid });
                parameters.Add(new SqlParameter() { ParameterName = "Action", Value = highlightData.action });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "highlight" });
                return _sqlClientService.Execute("Dim_GameTreeView", ConfigItems.Conn_Odds, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet GameTab()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "GameTab" });
                return _sqlClientService.Execute("Dim_GameTreeView", ConfigItems.Conn_Odds, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet Getmid(Guid uguid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = uguid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "UpdateTime" });
                return _sqlClientService.Execute("GetKeepAlive", ConfigItems.Conn_LogD, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet UserData(Int32 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                return _sqlClientService.Execute("User_BalanceData", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet UserBook(UserBook userBook)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = userBook.uid });
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = userBook.gmid });
                return _sqlClientService.Execute("front_UserBook", ConfigItems.Conn_Betgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetCasinoList(Casino cs)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = cs.guid });
                parameters.Add(new SqlParameter() { ParameterName = "webref", Value = cs.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "ctype", Value = cs.ctype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getcasinolist" });
                return _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetCasinoTabList(CasinoTabs ct)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ct.guid });
                parameters.Add(new SqlParameter() { ParameterName = "webref", Value = ct.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "casinotypeid", Value = ct.ctype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "gettablist" });
                //return _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                WriteLogAll("GetCasinoTabList+sp_casinodata", JsonConvert.SerializeObject(ct) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetCasinoTableList(CasinoTables ct)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ct.guid });
                parameters.Add(new SqlParameter() { ParameterName = "webref", Value = ct.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "gamemasterid", Value = ct.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "ismobile", Value = ct.ismob });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "gettablelist" });
                //return _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                WriteLogAll("GetCasinoTableList+sp_casinodata", JsonConvert.SerializeObject(ct) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet ButtonList(Int32 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "USerButton" });
                return _sqlClientService.Execute("update_Userbutton", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet ButtonUpdate(ButtonUpdate buttonUpdate)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = buttonUpdate.uid });
                parameters.Add(new SqlParameter() { ParameterName = "buttonId", Value = buttonUpdate.bid });
                parameters.Add(new SqlParameter() { ParameterName = "Buttonval", Value = buttonUpdate.bval });
                parameters.Add(new SqlParameter() { ParameterName = "ButtonText", Value = buttonUpdate.btxt });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Buttonupdateuser" });
                return _sqlClientService.Execute("update_Userbutton", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet ButtonListcs(Int32 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "USerButton" });
                return _sqlClientService.Execute("update_Userbuttoncasino", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet ButtonUpdatecs(ButtonUpdate buttonUpdate)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = buttonUpdate.uid });
                parameters.Add(new SqlParameter() { ParameterName = "buttonId", Value = buttonUpdate.bid });
                parameters.Add(new SqlParameter() { ParameterName = "Buttonval", Value = buttonUpdate.bval });
                parameters.Add(new SqlParameter() { ParameterName = "ButtonText", Value = buttonUpdate.btxt });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Buttonupdateuser" });
                return _sqlClientService.Execute("update_Userbuttoncasino", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet Matchdtlbet(Matchdtlbet matchdtlbet)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = matchdtlbet.uid });
                if (matchdtlbet.gtype == 1)
                {
                    parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = matchdtlbet.type });
                    DataSet ds = _sqlClientService.Execute("Front_ReportCurrentBet", ConfigItems.Conn_Betgroup, parameters);
                    WriteLogAll("Matchdtlbet+Front_ReportCurrentBet", JsonConvert.SerializeObject(matchdtlbet) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                else
                {
                    parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "casinobet" });
                    DataSet ds = _sqlClientService.Execute("front_reportcurrentbet", ConfigItems.Conn_Casinogroup, parameters);
                    WriteLogAll("Matchdtlbet+Front_ReportCurrentBet/casinobet", JsonConvert.SerializeObject(matchdtlbet) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
            }
            catch (Exception)
            {

                throw;
            }
        }
        public DataSet Matchdtlbetodds(String gmid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Gamelist", Value = gmid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "FrontCurrBet" });
                DataSet ds = _sqlClientService.Execute("Group1_GetGamedata", ConfigItems.Conn_Odds, parameters);
                WriteLogAll("Matchdtlbetodds+Group1_GetGamedata", gmid.ToString() + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet ChangePass(ChangePass changepass)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UserName", Value = changepass.uname });
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = changepass.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Password", Value = changepass.pass });
                parameters.Add(new SqlParameter() { ParameterName = "NewPassWord", Value = changepass.newpass });
                parameters.Add(new SqlParameter() { ParameterName = "Webdomain", Value = changepass.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "ip", Value = changepass.ip });
                parameters.Add(new SqlParameter() { ParameterName = "browserdetail", Value = changepass.bdetail });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "ChangePwd" });
                ds = _sqlClientService.Execute("EndUserLogin", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("ChangePass+EndUserLogin", JsonConvert.SerializeObject(changepass) + "Res:" + JsonConvert.SerializeObject(ds));
                //Select 0 as Id, 'Password Must not be same as previous password.' as msg ,UserId as  ChangedByLId,Username as  LUNameParent,@Password as UPassword,@Statement as [statement], @Ip as [ip], @browserdetail as[browserdetail],  0 as [ISSucessfull],Userid as [CreatedBy], UserName as  mainusername,Userid as mainuserid,1 srno,'' as comment,UserId as ChildMainUserId,Username as ChildMainUsername from tbl_UserMaster where Userid = @Userid
                try
                {
                    if (ds != null && ds.Tables != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Columns.Contains("id") && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                    {
                        var parameters1 = new List<SqlParameter>();
                        parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = ds.Tables[0].Rows[0]["userid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ChangedBy", Value = ds.Tables[0].Rows[0]["ChangedByLId"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "UNameParent", Value = ds.Tables[0].Rows[0]["LUNameParent"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "UPassword", Value = ds.Tables[0].Rows[0]["UPassword"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "ChangePwd" });
                        parameters1.Add(new SqlParameter() { ParameterName = "MsgId", Value = ds.Tables[0].Rows[0]["Id"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "message", Value = ds.Tables[0].Rows[0]["msg"].ToString() });
                        parameters1.Add(new SqlParameter() { ParameterName = "ip", Value = ds.Tables[0].Rows[0]["ip"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "browserdetail", Value = ds.Tables[0].Rows[0]["browserdetail"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ISSucessfull", Value = ds.Tables[0].Rows[0]["ISSucessfull"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "CreatedBy", Value = ds.Tables[0].Rows[0]["CreatedBy"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "MainUsername", Value = ds.Tables[0].Rows[0]["mainusername"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "mainUserid", Value = ds.Tables[0].Rows[0]["mainuserid"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ChildMainUserId", Value = ds.Tables[0].Rows[0]["ChildMainUserId"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "ChildMainUsername", Value = ds.Tables[0].Rows[0]["ChildMainUsername"] });
                        parameters1.Add(new SqlParameter() { ParameterName = "Action", Value = "AfterPwdchange" });
                        _sqlClientService.Execute("InsertChangePwdLog", ConfigItems.Conn_LogD, parameters1);
                    }
                }
                catch (Exception ex)
                {
                    //_errorLogService.WriteLogSe("ChangePass+EndUserLogin", JsonConvert.SerializeObject(changepass) + "Res:" + JsonConvert.SerializeObject(ds), ex.ToString());
                }

                //var parameters1 = new List<SqlParameter>();
                //parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = "00000000-0000-0000-0000-000000000000" });
                //parameters1.Add(new SqlParameter() { ParameterName = "ChangedBy", Value = "00000000-0000-0000-0000-000000000000" });
                //parameters1.Add(new SqlParameter() { ParameterName = "UNameParent", Value = changepass.uname });
                //parameters1.Add(new SqlParameter() { ParameterName = "UPassword", Value = changepass.pass });
                //parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "ChangePwd" });
                //parameters1.Add(new SqlParameter() { ParameterName = "MsgId", Value = ds.Tables[0].Rows[0]["id"] });
                //parameters1.Add(new SqlParameter() { ParameterName = "message", Value = ds.Tables[0].Rows[0]["msg"].ToString() });
                //parameters1.Add(new SqlParameter() { ParameterName = "ip", Value = changepass.ip });
                //parameters1.Add(new SqlParameter() { ParameterName = "browserdetail", Value = changepass.bdetail });
                //parameters1.Add(new SqlParameter() { ParameterName = "ISSucessfull", Value = ds.Tables[0].Rows[0]["id"].ToString() == "1" ? true : false });
                //_sqlClientService.Execute("InsertChangePwdLog", ConfigItems.Conn_LogD, parameters1);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet CasinoUserBook(CasinoUserBook casinoUserBook)
        {
            try
            {
                if (casinoUserBook.gtype == "teen" || casinoUserBook.gtype == "teen20b" || casinoUserBook.gtype == "teen8" || casinoUserBook.gtype == "teen20" || casinoUserBook.gtype == "teen9" || casinoUserBook.gtype == "teen6" || casinoUserBook.gtype == "teen3")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("teen_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "baccarat" || casinoUserBook.gtype == "baccarat2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("baccarat_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    ////_errorLogService.WriteLog("CasinoUserBook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "lucky7" || casinoUserBook.gtype == "lucky7eu" || casinoUserBook.gtype == "lucky7eu2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("lucky7_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+lucky7_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "poker" || casinoUserBook.gtype == "poker20" || casinoUserBook.gtype == "poker6")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("poker_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+poker_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "dt6" || casinoUserBook.gtype == "dt20" || casinoUserBook.gtype == "dt202" || casinoUserBook.gtype == "dtl20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("dt_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+dt_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "aaa" || casinoUserBook.gtype == "btable" || casinoUserBook.gtype == "aaa2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("bc_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+bc_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "worli" || casinoUserBook.gtype == "worli2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("worli_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+worli_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "war" || casinoUserBook.gtype == "paasa")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("other_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "3cardj")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("cardj_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    return ds;
                }
                if (casinoUserBook.gtype == "cmeter" || casinoUserBook.gtype == "cmatch20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("sports_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    WriteLogAll("CasinoUserBook+sports_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "card32" || casinoUserBook.gtype == "card32eu")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("card32_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+card32_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "ab20" || casinoUserBook.gtype == "abj" || casinoUserBook.gtype == "ab3")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("ab_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+ab_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "queen")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("queen_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+queen_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "lottcard")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("lottery_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+lottery_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "race20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("race_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+race_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "trap" || casinoUserBook.gtype == "patti2" || casinoUserBook.gtype == "trio" || casinoUserBook.gtype == "teensin" || casinoUserBook.gtype == "teenmuf" || casinoUserBook.gtype == "race17")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("other1_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "notenum" || casinoUserBook.gtype == "teen2024" || casinoUserBook.gtype == "teen1" || casinoUserBook.gtype == "teen120")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("other2_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                return null;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet VCasinoUserBook(CasinoUserBook casinoUserBook)
        {
            try
            {
                if (casinoUserBook.gtype == "vteen" || casinoUserBook.gtype == "vteen20b" || casinoUserBook.gtype == "vteen8" || casinoUserBook.gtype == "vteen20" || casinoUserBook.gtype == "vteen9" || casinoUserBook.gtype == "vteen6")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("z_teen_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vbaccarat" || casinoUserBook.gtype == "vbaccarat2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_baccarat_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    ////_errorLogService.WriteLog("CasinoUserBook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vlucky7" || casinoUserBook.gtype == "lucky7eu" || casinoUserBook.gtype == "lucky7eu2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_lucky7_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+lucky7_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "poker" || casinoUserBook.gtype == "poker20" || casinoUserBook.gtype == "poker6")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_poker_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+poker_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vdt6" || casinoUserBook.gtype == "vdt20" || casinoUserBook.gtype == "dt202" || casinoUserBook.gtype == "vdtl20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_dt_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+dt_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vaaa" || casinoUserBook.gtype == "vbtable")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_bc_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+bc_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "worli" || casinoUserBook.gtype == "worli2")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_worli_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+worli_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "war" || casinoUserBook.gtype == "paasa")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_other_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "3cardj")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_cardj_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    return ds;
                }
                if (casinoUserBook.gtype == "cmeter" || casinoUserBook.gtype == "cmatch20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_sports_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+sports_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vcard32" || casinoUserBook.gtype == "card32eu")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_card32_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+card32_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "ab20" || casinoUserBook.gtype == "abj")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_ab_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+ab_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vqueen")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_queen_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+queen_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "lottcard")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_lottery_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+lottery_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vrace20")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_race_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+race_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vtrap" || casinoUserBook.gtype == "patti2" || casinoUserBook.gtype == "vtrio" || casinoUserBook.gtype == "teensin" || casinoUserBook.gtype == "vteenmuf" || casinoUserBook.gtype == "vrace17")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_other1_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (casinoUserBook.gtype == "vnotenum" || casinoUserBook.gtype == "vteen2024" || casinoUserBook.gtype == "vteen1" || casinoUserBook.gtype == "vteen120")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
                    DataSet ds = _sqlClientService.Execute("Z_other2_userbook", ConfigItems.Conn_Casinogroup, parameters);
                    //_errorLogService.WriteLog("CasinoUserBook+other_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                return null;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AccountStatementAcc(AccountStatement accountStatement)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accountStatement.uid });
                parameters.Add(new SqlParameter() { ParameterName = "dtfrom", Value = accountStatement.fdt });
                parameters.Add(new SqlParameter() { ParameterName = "dtto", Value = accountStatement.tdt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userreport" });
                DataSet ds = _sqlClientService.Execute("user_reportaccountstatement", ConfigItems.Conn_AccDgroup, parameters);
                //WriteLogAll("AccountStatementAcc+user_reportaccountstatement", accountStatement + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public DataSet AccountStatementCS(AccountStatement accountStatement)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accountStatement.uid });
                parameters.Add(new SqlParameter() { ParameterName = "dtfrom", Value = accountStatement.fdt });
                parameters.Add(new SqlParameter() { ParameterName = "dtto", Value = accountStatement.tdt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userreport" });
                DataSet ds = _sqlClientService.Execute("front_reportaccountstatement", ConfigItems.Conn_Casinogroup, parameters);
                WriteLogAll("AccountStatementCS+front_reportaccountstatement", accountStatement + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public DataSet AccountStatementBet(AccountStatement accountStatement)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accountStatement.uid });
                parameters.Add(new SqlParameter() { ParameterName = "dtfrom", Value = accountStatement.fdt });
                parameters.Add(new SqlParameter() { ParameterName = "dtto", Value = accountStatement.tdt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userreport" });
                DataSet ds = _sqlClientService.Execute("front_reportaccountstatement", ConfigItems.Conn_Betgroup, parameters);
                WriteLogAll("AccountStatementBet+front_reportaccountstatement", accountStatement + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public DataSet AccountStatementParty3(AccountStatement accountStatement)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accountStatement.uid });
                parameters.Add(new SqlParameter() { ParameterName = "dtfrom", Value = accountStatement.fdt });
                parameters.Add(new SqlParameter() { ParameterName = "dtto", Value = accountStatement.tdt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "tpcasino" });
                DataSet ds = _sqlClientService.Execute("user_reportaccountstatement", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("AccountStatementParty3+user_reportaccountstatement+tpcasino", JsonConvert.SerializeObject(accountStatement) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public DataSet AccStatePopup(AccStatePopup accStatePopup)
        {
            try
            {
                if (accStatePopup.dtype.ToString().ToLower() == "ss")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = accStatePopup.gmid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = accStatePopup.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accStatePopup.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = accStatePopup.gtype });
                    parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "sportpopup" });
                    DataSet ds = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Betgroup, parameters);
                    //_errorLogService.WriteLog("AccStatePopup", accStatePopup + "Res:" + JsonConvert.SerializeObject(ds));
                    return ds;
                }
                if (accStatePopup.dtype.ToString().ToLower() == "cs")
                {
                    var parameters = new List<SqlParameter>();
                    parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = accStatePopup.gmid });
                    parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = accStatePopup.mid });
                    parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accStatePopup.uid });
                    parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = accStatePopup.gtype });
                    parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "casionpopup" });
                    DataSet ds1 = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Casinogroup, parameters);
                    WriteLogAll("AccStatePopup+front_reportaccountpopup", JsonConvert.SerializeObject(accStatePopup) + "Res:" + JsonConvert.SerializeObject(ds1));
                    return ds1;
                }
                if (accStatePopup.dtype.ToString().ToLower() == "ds")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "gameid", Value = accStatePopup.gmid });
                    parameters1.Add(new SqlParameter() { ParameterName = "marketid", Value = accStatePopup.mid });
                    parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = accStatePopup.uid });
                    //parameters1.Add(new SqlParameter() { ParameterName = "gametype", Value = accStatePopup.gtype });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "daba" });
                    DataSet ds2 = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Daba, parameters1);
                    //_errorLogService.WriteLog("AccStatePopup+front_reportaccountpopup", accStatePopup + "Res:" + JsonConvert.SerializeObject(ds2));
                    return ds2;
                }
                if (accStatePopup.dtype.ToString().ToLower() == "d1s")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "gameid", Value = accStatePopup.gmid });
                    parameters1.Add(new SqlParameter() { ParameterName = "marketid", Value = accStatePopup.mid });
                    parameters1.Add(new SqlParameter() { ParameterName = "userid", Value = accStatePopup.uid });
                    //parameters1.Add(new SqlParameter() { ParameterName = "gametype", Value = accStatePopup.gtype });
                    parameters1.Add(new SqlParameter() { ParameterName = "statement", Value = "Dream" });
                    DataSet ds2 = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Dream, parameters1);
                    //_errorLogService.WriteLog("AccStatePopup+front_reportaccountpopup", accStatePopup + "Res:" + JsonConvert.SerializeObject(ds2));
                    return ds2;
                }
                return null;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AccDreamBet(AccDabaBet accDabaBet)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accDabaBet.uid });
                parameters.Add(new SqlParameter() { ParameterName = "UContestId", Value = accDabaBet.btid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "BetTeam" });
                DataSet ds = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Dream, parameters);
                //_errorLogService.WriteLog("AccDreamBet+front_reportaccountpopup", accDabaBet + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AccDabaBet(AccDabaBet accDabaBet)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = accDabaBet.uid });
                parameters.Add(new SqlParameter() { ParameterName = "BetId", Value = accDabaBet.btid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "PlayerList" });
                DataSet ds = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Daba, parameters);
                //_errorLogService.WriteLog("AccDabaBet+front_reportaccountpopup", accDabaBet + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AutoCon(Int64 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "GetAutocofirm" });
                return _sqlClientService.Execute("GetUserData", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AutoconUpdate(AutoconUpdate autoconUpdate)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = autoconUpdate.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Autoconfirm", Value = autoconUpdate.autocon });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "UpdateAutoconfirm" });
                return _sqlClientService.Execute("GetUserData", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet OtherCasino(OtherCasino otherCasino)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "ctype", Value = otherCasino.ctype });
                parameters.Add(new SqlParameter() { ParameterName = "ismobile", Value = otherCasino.mobile });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "othercasinolist" });
                return _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoMaster, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet FancyBook(FancyBook fancyBook)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = fancyBook.uid });
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = fancyBook.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = fancyBook.mid });
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = fancyBook.sid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "frontbook" });
                DataSet ds = _sqlClientService.Execute("Front_FancyUserBook", ConfigItems.Conn_Fancygroup, parameters);
                WriteLogAll("Front_FancyUserBook+frontbook", JsonConvert.SerializeObject(fancyBook) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public DataSet KhadoBook(FancyBook fancyBook)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = fancyBook.uid });
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = fancyBook.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = fancyBook.mid });
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = fancyBook.sid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "frontbook" });
                DataSet ds = _sqlClientService.Execute("Front_KhadoUserBook", ConfigItems.Conn_Fancygroup, parameters);
                //_errorLogService.WriteLog("KhadoBook", fancyBook + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet MeterBook(FancyBook fancyBook)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = fancyBook.uid });
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = fancyBook.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = fancyBook.mid });
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = fancyBook.sid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "frontbook" });
                return _sqlClientService.Execute("Front_MeterUserBook", ConfigItems.Conn_Fancygroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet MarketAnalysis(Int32 uid)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UserId", Value = uid });
                ds = _sqlClientService.Execute("front_Marketanalysis", ConfigItems.Conn_Betgroup, parameters);
                WriteLogAll("MarketAnalysis+front_Marketanalysis", uid.ToString() + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet MarketAnalysisReportodds(String gmid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Gamelist", Value = gmid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "FrontMarketAna" });
                DataSet ds = _sqlClientService.Execute("Group1_GetGamedata", ConfigItems.Conn_Odds, parameters);
                WriteLogAll("MarketAnalysis+Group1_GetGamedata", gmid.ToString() + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetM(GetM getM)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "eventtypeid", Value = getM.etid });
                return _sqlClientService.Execute("front_Marketanalysis", ConfigItems.Conn_Betgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GameSer(GameSer gameSer)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "eventname", Value = gameSer.ename });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "gamesearch" });
                return _sqlClientService.Execute("Dim_GameSearch", ConfigItems.Conn_Odds, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet History(History history)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "type", Value = history.type });
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = history.uid });
                parameters.Add(new SqlParameter() { ParameterName = "fromdt", Value = history.fdt });
                parameters.Add(new SqlParameter() { ParameterName = "todt", Value = history.tdt });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userloghistory" });
                return _sqlClientService.Execute("getHistory", ConfigItems.Conn_LogD, parameters);
            }
            catch (Exception)
            {
                throw;
            }
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
        public DataSet TvCsData(TvCsData tvCsData)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = tvCsData.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "mobiletype", Value = tvCsData.mtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "casinotv" });
                ds = _sqlClientService.Execute("tv_userdata", ConfigItems.Conn_Casino, parameters);
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetTPCSUserData(Int32 uid, String ctype, String statement)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UserId", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "CasinoType", Value = ctype });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = statement });
                return _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetTPCSUserMster(TPCSUserMaster objtpcsum)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = objtpcsum.guid });
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = objtpcsum.uid });
                parameters.Add(new SqlParameter() { ParameterName = "username", Value = objtpcsum.uname });
                parameters.Add(new SqlParameter() { ParameterName = "general", Value = objtpcsum.gen });
                parameters.Add(new SqlParameter() { ParameterName = "currency", Value = objtpcsum.curr });
                parameters.Add(new SqlParameter() { ParameterName = "webdomain", Value = objtpcsum.webdom });
                parameters.Add(new SqlParameter() { ParameterName = "webref", Value = objtpcsum.webref });
                parameters.Add(new SqlParameter() { ParameterName = "partnership", Value = objtpcsum.pship });
                parameters.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = objtpcsum.pshiptype });
                parameters.Add(new SqlParameter() { ParameterName = "casinocode", Value = objtpcsum.cscode });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userinsert" });
                return _sqlClientService.Execute("usermaster", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginEZ(EZLogin ez)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ez.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("ezugi_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginSS(SSLogin ss)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ss.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("ss_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginQT(QTLogin qt)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = qt.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("qt_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginEV(EZLogin ev)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ev.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("evolution_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginR(QTLogin qt)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = qt.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("runner_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginCF(CFLogin cf)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = cf.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("cockfight_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet updatetoken(String guid, String newtoken, String token)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
            parameters.Add(new SqlParameter() { ParameterName = "token", Value = token });
            parameters.Add(new SqlParameter() { ParameterName = "newtoken", Value = newtoken });
            parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userauth" });
            return _sqlClientService.Execute("ss_login", ConfigItems.Conn_CasinoTP, parameters);
        }
        public DataSet Rules()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "selectruleuser" });
                return _sqlClientService.Execute("sp_rulesmaster", ConfigItems.Conn_Odds, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Casinores(Casinores casinores)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "dt1", Value = casinores.dt });
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinores.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "resultlist" });
                return _sqlClientService.Execute("SP_reportteenresult", ConfigItems.Conn_Casinogroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Casino()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "OwnCasinoList" });
                return _sqlClientService.Execute("getreportdata", ConfigItems.Conn_Casino, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet CheckCasinovs(CheckCasinovs checkCasinovs)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Gameid", Value = checkCasinovs.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "IsCasinohide" });
                return _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet TPReport(TPReport tPReport)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = tPReport.uid });
                parameters.Add(new SqlParameter() { ParameterName = "casinotype", Value = tPReport.ctype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userbetlist" });
                return _sqlClientService.Execute("reportmaster", ConfigItems.Conn_CasinoMgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet TPgtypelist()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getgametypelist" });
                return _sqlClientService.Execute("reportmaster", ConfigItems.Conn_CasinoM, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet TPAllReport(TPAllReport tPAllReport)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = tPAllReport.uid });
                parameters.Add(new SqlParameter() { ParameterName = "date", Value = tPAllReport.dt });
                parameters.Add(new SqlParameter() { ParameterName = "casinotype", Value = tPAllReport.ctype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userbetlist" });
                DataSet ds = _sqlClientService.Execute("reportmaster_userwise", ConfigItems.Conn_CasinoMgroup, parameters);
                //_errorLogService.WriteLog("TPAllReport+reportmaster_userwise+userbetlist", JsonConvert.SerializeObject(tPAllReport) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet TPcurrentbets(Int32 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "currentbetlist" });
                return _sqlClientService.Execute("reportmaster_userwise", ConfigItems.Conn_CasinoMgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet TPcurrentbets1(Int32 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "currentbetlist1" });
                return _sqlClientService.Execute("reportmaster_userwise", ConfigItems.Conn_CasinoMgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet RunnerViewmore(RunnerViewmore runnerViewmore)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "txnid", Value = runnerViewmore.txnid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "runnerviewmore" });
                DataSet ds = _sqlClientService.Execute("reportmaster", ConfigItems.Conn_CasinoM, parameters);
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet AcceptRules(Guid uu_id)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "SaGuId", Value = uu_id });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "AgreeRules" });
                return _sqlClientService.Execute("GetUserData", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet CasinoRules(CasinoRules casinoRules)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoRules.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "CasinoRules" });
                DataSet ds = _sqlClientService.Execute("getreportdata", ConfigItems.Conn_Casinogroup, parameters);
                //WriteLogAll("CasinoRules+getreportdata", JsonConvert.SerializeObject(casinoRules) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Loginpoptheball(String guid, Int64 uid)
        {
            try
            {
                DataSet ds = new DataSet();
                DataSet ds1 = new DataSet();
                DataSet ds2 = new DataSet();
                DataSet ds3 = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userlogin" });
                ds = _sqlClientService.Execute("popball_login", ConfigItems.Conn_CasinoMaster, parameters);
                //_errorLogService.WriteLog("Loginpoptheball+TpCasinoUserData+userlogin", guid + "Res:" + JsonConvert.SerializeObject(ds));

                if (ds != null && ds.Tables != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Columns.Contains("id") && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters1.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "pop-the-ball" });
                    parameters1.Add(new SqlParameter() { ParameterName = "Statement", Value = "ExistUserData" });
                    ds1 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters1);
                    //_errorLogService.WriteLog("Loginpoptheball+TpCasinoUserData+ExistUserData", guid + "Res:" + JsonConvert.SerializeObject(ds1));
                    return ds1;
                }
                else
                {
                    var parameters2 = new List<SqlParameter>();
                    parameters2.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters2.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "pop-the-ball" });
                    parameters2.Add(new SqlParameter() { ParameterName = "Statement", Value = "GetUserData" });
                    ds2 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters2);
                    //_errorLogService.WriteLog("Loginpoptheball+TpCasinoUserData+GetUserData", guid + "Res:" + JsonConvert.SerializeObject(ds2));
                    //select 1 as Id,'ok' as,msg,u.userid,u.uguid,u.userName,u.WebRef,u.webdomain,u.PartnershipType, general, exposer,'INR' as Currency,@JSon Partnership
                    if (ds2 != null && ds2.Tables != null && ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0 && ds2.Tables[0].Columns.Contains("id") && ds2.Tables[0].Rows[0]["id"].ToString() == "1")
                    {//@guid,@userid,@username,@general,@currency ,@webdomain,@webref,@partnership,@partnershiptype,@casinocode
                        var parameters3 = new List<SqlParameter>();
                        parameters3.Add(new SqlParameter() { ParameterName = "guid", Value = ds2.Tables[0].Rows[0]["uguid"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "userid", Value = ds2.Tables[0].Rows[0]["UserId"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "username", Value = ds2.Tables[0].Rows[0]["userName"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "general", Value = ds2.Tables[0].Rows[0]["general"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "currency", Value = ds2.Tables[0].Rows[0]["Currency"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webdomain", Value = ds2.Tables[0].Rows[0]["webdomain"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webref", Value = ds2.Tables[0].Rows[0]["WebRef"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnership", Value = ds2.Tables[0].Rows[0]["Partnership"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = ds2.Tables[0].Rows[0]["PartnershipType"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "casinocode", Value = "pop-the-ball" });
                        parameters3.Add(new SqlParameter() { ParameterName = "Statement", Value = "userinsert" });
                        ds3 = _sqlClientService.Execute("usermaster", ConfigItems.Conn_CasinoMaster, parameters3);
                        //_errorLogService.WriteLog("Loginpoptheball+usermaster+userinsert", guid + "Res:" + JsonConvert.SerializeObject(ds3));
                    }
                    return ds2;
                }

                return ds1;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet Popballtoken(String guid, String seid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "token", Value = seid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "logintoken" });
                DataSet ds = _sqlClientService.Execute("popball_login", ConfigItems.Conn_CasinoMaster, parameters);
                //_errorLogService.WriteLog("Popballtoken+popball_login", seid + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Loginludo(String guid, Int64 uid)
        {
            try
            {
                DataSet ds = new DataSet();
                DataSet ds1 = new DataSet();
                DataSet ds2 = new DataSet();
                DataSet ds3 = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userlogin" });
                ds = _sqlClientService.Execute("ludoclub_login", ConfigItems.Conn_CasinoMaster, parameters);
                //_errorLogService.WriteLog("Loginludo+TpCasinoUserData+userlogin", guid + "Res:" + JsonConvert.SerializeObject(ds));

                if (ds != null && ds.Tables != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Columns.Contains("id") && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters1.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "ludo-club" });
                    parameters1.Add(new SqlParameter() { ParameterName = "Statement", Value = "ExistUserData" });
                    ds1 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters1);
                    //_errorLogService.WriteLog("Loginludo+TpCasinoUserData+ExistUserData", guid + "Res:" + JsonConvert.SerializeObject(ds1));
                    return ds1;
                }
                else
                {
                    var parameters2 = new List<SqlParameter>();
                    parameters2.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters2.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "ludo-club" });
                    parameters2.Add(new SqlParameter() { ParameterName = "Statement", Value = "GetUserData" });
                    ds2 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters2);
                    //_errorLogService.WriteLog("Loginludo+TpCasinoUserData+GetUserData", guid + "Res:" + JsonConvert.SerializeObject(ds2));
                    //select 1 as Id,'ok' as,msg,u.userid,u.uguid,u.userName,u.WebRef,u.webdomain,u.PartnershipType, general, exposer,'INR' as Currency,@JSon Partnership
                    if (ds2 != null && ds2.Tables != null && ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0 && ds2.Tables[0].Columns.Contains("id") && ds2.Tables[0].Rows[0]["id"].ToString() == "1")
                    {//@guid,@userid,@username,@general,@currency ,@webdomain,@webref,@partnership,@partnershiptype,@casinocode
                        var parameters3 = new List<SqlParameter>();
                        parameters3.Add(new SqlParameter() { ParameterName = "guid", Value = ds2.Tables[0].Rows[0]["uguid"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "userid", Value = ds2.Tables[0].Rows[0]["UserId"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "username", Value = ds2.Tables[0].Rows[0]["userName"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "general", Value = ds2.Tables[0].Rows[0]["general"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "currency", Value = ds2.Tables[0].Rows[0]["Currency"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webdomain", Value = ds2.Tables[0].Rows[0]["webdomain"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webref", Value = ds2.Tables[0].Rows[0]["WebRef"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnership", Value = ds2.Tables[0].Rows[0]["Partnership"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = ds2.Tables[0].Rows[0]["PartnershipType"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "casinocode", Value = "ludo-club" });
                        parameters3.Add(new SqlParameter() { ParameterName = "Statement", Value = "userinsert" });
                        ds3 = _sqlClientService.Execute("usermaster", ConfigItems.Conn_CasinoMaster, parameters3);
                        //_errorLogService.WriteLog("Loginludo+usermaster+userinsert", guid + "Res:" + JsonConvert.SerializeObject(ds3));
                    }
                    return ds2;
                }

                return ds1;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet Ludotoken(String guid, String seid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "token", Value = seid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "logintoken" });
                DataSet ds = _sqlClientService.Execute("ludoclub_login", ConfigItems.Conn_CasinoMaster, parameters);
                //_errorLogService.WriteLog("Ludotoken+Loginludo_login", seid + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet WorliPana(WorliPana worliPana)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = worliPana.sid });
                parameters.Add(new SqlParameter() { ParameterName = "userrate", Value = worliPana.urate });
                DataSet ds = _sqlClientService.Execute("worli_countpana", ConfigItems.Conn_Casinogroup, parameters);
                //_errorLogService.WriteLog("WorliPana+worli_countpana", JsonConvert.SerializeObject(worliPana) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet WorliRule(WorliRule worliRule)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = worliRule.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = worliRule.sid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "worlirules" });
                DataSet ds = _sqlClientService.Execute("front_gamerules", ConfigItems.Conn_Casinogroup, parameters);
                //_errorLogService.WriteLog("WorliRule+front_gamerules", JsonConvert.SerializeObject(worliRule) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;

            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Cricketv3popup(Cricketv3popup cricketv3Popup)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "marketid1", Value = cricketv3Popup.mid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getscore" });
                ds = _sqlClientService.Execute("cricketv3masterGetdata", ConfigItems.Conn_Odds, parameters);
                //_errorLogService.WriteLog("Cricketv3popup+cricketv3masterGetdata", JsonConvert.SerializeObject(cricketv3Popup) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet CricketSopopup(Cricketv3popup cricketv3Popup)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "marketid1", Value = cricketv3Popup.mid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getscore" });
                ds = _sqlClientService.Execute("Cricketsomastergetdata", ConfigItems.Conn_Odds, parameters);
                //_errorLogService.WriteLog("Cricketv3popup+cricketv3masterGetdata", JsonConvert.SerializeObject(cricketv3Popup) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Cricketv3popupsoda(Cricketv3popup cricketv3Popup)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = cricketv3Popup.mid });
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = cricketv3Popup.uid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "cricketv3popup" });
                ds = _sqlClientService.Execute("front_reportaccountpopup", ConfigItems.Conn_Betgroup, parameters);
                //_errorLogService.WriteLog("Cricketv3popupsoda+front_reportaccountpopup", JsonConvert.SerializeObject(cricketv3Popup) + "Res:" + JsonConvert.SerializeObject(ds));
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
                ds = _sqlClientService.Execute("WolfUserCreation", ConfigItems.Conn_AccDgroup, parameters);
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
                DataSet ds = _sqlClientService.Execute("InsertNewUserData", ConfigItems.Conn_Casinogroup, parameters);
                //_errorLogService.WriteLog("UsercreateData+InsertNewUserData", json + "Res:" + JsonConvert.SerializeObject(ds));
                if (ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Json", Value = json });
                    _sqlClientService.Execute("InsertNewUserData", ConfigItems.Conn_Betgroup, parameters1);
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
        public DataSet Paymentval(Paymentval paymentval)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gUserId", Value = paymentval.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Amount", Value = paymentval.amt });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Generatecode" });
                DataSet ds = _sqlClientService.Execute("Royal_Paymentvalidation", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("Paymentval+Royal_Paymentvalidation", JsonConvert.SerializeObject(paymentval) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Createreq(Createreq rWithdraw)
        {
            try
            {//@ChildUserid,@AccNumber,@AccName,@Ifscode,@BankName,@AccountType,@Remark,@UserName, @gUserId
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gUserId", Value = rWithdraw.guid });
                parameters.Add(new SqlParameter() { ParameterName = "AccNumber", Value = rWithdraw.anumber });
                parameters.Add(new SqlParameter() { ParameterName = "AccName", Value = rWithdraw.aname });
                parameters.Add(new SqlParameter() { ParameterName = "Ifscode", Value = rWithdraw.ifsc });
                parameters.Add(new SqlParameter() { ParameterName = "BankName", Value = rWithdraw.bname });
                parameters.Add(new SqlParameter() { ParameterName = "AccountType", Value = rWithdraw.atype });
                parameters.Add(new SqlParameter() { ParameterName = "Amount", Value = rWithdraw.amt });
                parameters.Add(new SqlParameter() { ParameterName = "Remark", Value = rWithdraw.rem });
                parameters.Add(new SqlParameter() { ParameterName = "express", Value = rWithdraw.exp });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Insert" });
                DataSet ds = _sqlClientService.Execute("Royal_WithDraw", ConfigItems.Conn_AccDgroup, parameters);
                //WriteLogAll("Createreq+Royal_WithDraw", JsonConvert.SerializeObject(rWithdraw) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Getreq(String uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gUserId", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Getdatabyid" });
                DataSet ds = _sqlClientService.Execute("Royal_WithDraw", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("Getreq+Royal_WithDraw", uid.ToString() + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet LoginTeen(String guid, Int64 uid)
        {
            try
            {
                DataSet ds = new DataSet();
                DataSet ds1 = new DataSet();
                DataSet ds2 = new DataSet();
                DataSet ds3 = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "userlogin" });
                ds = _sqlClientService.Execute("popball_login", ConfigItems.Conn_CasinoMaster, parameters);
                //_errorLogService.WriteLog("LoginTeen+TpCasinoUserData+userlogin", guid + "Res:" + JsonConvert.SerializeObject(ds));

                if (ds != null && ds.Tables != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0 && ds.Tables[0].Columns.Contains("id") && ds.Tables[0].Rows[0]["id"].ToString() == "1")
                {
                    var parameters1 = new List<SqlParameter>();
                    parameters1.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters1.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "pop-the-ball" });
                    parameters1.Add(new SqlParameter() { ParameterName = "Statement", Value = "ExistUserData" });
                    ds1 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters1);
                    //_errorLogService.WriteLog("LoginTeen+TpCasinoUserData+ExistUserData", guid + "Res:" + JsonConvert.SerializeObject(ds1));
                    return ds1;
                }
                else
                {
                    var parameters2 = new List<SqlParameter>();
                    parameters2.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                    parameters2.Add(new SqlParameter() { ParameterName = "CasinoType", Value = "pop-the-ball" });
                    parameters2.Add(new SqlParameter() { ParameterName = "Statement", Value = "GetUserData" });
                    ds2 = _sqlClientService.Execute("TpCasinoUserData", ConfigItems.Conn_AccDgroup, parameters2);
                    //_errorLogService.WriteLog("LoginTeen+TpCasinoUserData+GetUserData", guid + "Res:" + JsonConvert.SerializeObject(ds2));
                    //select 1 as Id,'ok' as,msg,u.userid,u.uguid,u.userName,u.WebRef,u.webdomain,u.PartnershipType, general, exposer,'INR' as Currency,@JSon Partnership
                    if (ds2 != null && ds2.Tables != null && ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0 && ds2.Tables[0].Columns.Contains("id") && ds2.Tables[0].Rows[0]["id"].ToString() == "1")
                    {//@guid,@userid,@username,@general,@currency ,@webdomain,@webref,@partnership,@partnershiptype,@casinocode
                        var parameters3 = new List<SqlParameter>();
                        parameters3.Add(new SqlParameter() { ParameterName = "guid", Value = ds2.Tables[0].Rows[0]["uguid"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "userid", Value = ds2.Tables[0].Rows[0]["UserId"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "username", Value = ds2.Tables[0].Rows[0]["userName"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "general", Value = ds2.Tables[0].Rows[0]["general"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "currency", Value = ds2.Tables[0].Rows[0]["Currency"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webdomain", Value = ds2.Tables[0].Rows[0]["webdomain"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "webref", Value = ds2.Tables[0].Rows[0]["WebRef"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnership", Value = ds2.Tables[0].Rows[0]["Partnership"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = ds2.Tables[0].Rows[0]["PartnershipType"] });
                        parameters3.Add(new SqlParameter() { ParameterName = "casinocode", Value = "pop-the-ball" });
                        parameters3.Add(new SqlParameter() { ParameterName = "Statement", Value = "userinsert" });
                        ds3 = _sqlClientService.Execute("usermaster", ConfigItems.Conn_CasinoMaster, parameters3);
                        //_errorLogService.WriteLog("LoginTeen+usermaster+userinsert", guid + "Res:" + JsonConvert.SerializeObject(ds3));
                    }
                    return ds2;
                }

                return ds1;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet Gamedata2(GameData gameData)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = gameData.gmid });
                DataSet ds = _sqlClientService.Execute("gamedata", ConfigItems.Conn_Redis, parameters);
                //_errorLogService.WriteLog("Gamedata2+gamedata+userlogin", gameData + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet GetCop(Guid guid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = guid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "getCoupen" });
                DataSet ds = _sqlClientService.Execute("Royal_GenerateTicket", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("Gamedata2+gamedata+userlogin", gameData + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet RptDelBet(DeleteBetReport dbr)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = dbr.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Date", Value = dbr.dt });
                //parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userbetlist" });
                if (dbr.type == 1)
                    return _sqlClientService.Execute("Front_ReportDeleteBet", ConfigItems.Conn_Betgroup, parameters);
                else
                    return _sqlClientService.Execute("Front_ReportDeleteBet", ConfigItems.Conn_Casinogroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginTGS(TGSLogin tgs)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = tgs.guid });
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = tgs.gid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("tgs_master", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginVL(EZLogin ez)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = ez.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("vegas_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet LoginES(ESLogin es)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = es.guid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("slot_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Fancypopup(Fancypopup dbr)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = dbr.uid });
                parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = dbr.mid });
                parameters.Add(new SqlParameter() { ParameterName = "sectionid", Value = dbr.sid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "frontbook" });
                DataSet ds = _sqlClientService.Execute("Front_FancyUserBook", ConfigItems.Conn_CasinoFancygroup, parameters);
                //WriteLogAll("Fancypopup+Front_FancyUserBook", "Req:" + JsonConvert.SerializeObject(dbr) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                WriteLogAll("Fancypopup+Front_FancyUserBook", "Req:" + ex.ToString());
                throw;
            }
        }
        public DataSet Casinolisttest()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getcasinolist1" });
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                WriteLogAll("Casinolisttest", "Req:" + JsonConvert.SerializeObject(ds));
                return ds;
                //parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getcasinolist" });
                //return _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Providerlist()
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getproviderlist" });
                //DataSet ds = _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Categorylist(Categorylist dbr)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "providerid", Value = dbr.pid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getcategorylist" });
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                //DataSet ds = _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
                WriteLogAll("Categorylist+sp_casinodata", "Req:" + JsonConvert.SerializeObject(dbr) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                WriteLogAll("Categorylist+sp_casinodata", "Req:" + ex.ToString());
                throw;
            }
        }
        public DataSet Slotlist(Slotlist dbr)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "categoryid", Value = dbr.cid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "getslotlist" });
                //DataSet ds = _sqlClientService.Execute("sp_testcasinodata", ConfigItems.Conn_CasinoM, parameters);
                DataSet ds = _sqlClientService.Execute("sp_casinodata", ConfigItems.Conn_CasinoM, parameters);
                WriteLogAll("Slotlist+sp_casinodata", "Req:" + JsonConvert.SerializeObject(dbr) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                WriteLogAll("Slotlist+sp_casinodata", "Req:" + ex.ToString());
                throw;
            }
        }
        public DataSet GetRules(GetRules vt)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "GameType", Value = vt.gtype });
            parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "getrulesFront" });
            DataSet ds = _sqlClientService.Execute("Insert_Rules", ConfigItems.Conn_Casino, parameters);
            //_errorLogService.WriteLog("Framelogin", JsonConvert.SerializeObject(vt) + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet KbcUserBook(CasinoUserBook casinoUserBook)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = casinoUserBook.uid });
            parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = casinoUserBook.mid });
            parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = casinoUserBook.gtype });
            DataSet ds = _sqlClientService.Execute("otherkbc_userbook", ConfigItems.Conn_Casinogroup, parameters);
            WriteLogAll("KbcUserBook+otherkbc_userbook", JsonConvert.SerializeObject(casinoUserBook) + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet KbcPayout(KbcPayout kbcPayout)
        {
            var parameters = new List<SqlParameter>();
            parameters.Add(new SqlParameter() { ParameterName = "userid", Value = kbcPayout.uid });
            parameters.Add(new SqlParameter() { ParameterName = "marketid", Value = kbcPayout.mid });
            parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = kbcPayout.gtype });
            parameters.Add(new SqlParameter() { ParameterName = "pid", Value = kbcPayout.pid });
            parameters.Add(new SqlParameter() { ParameterName = "statement", Value = kbcPayout.sta });
            DataSet ds = _sqlClientService.Execute("otherkbc_payout", ConfigItems.Conn_Casinogroup, parameters);
            WriteLogAll("KbcPayout+otherkbc_payout", JsonConvert.SerializeObject(kbcPayout) + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet LaunchCasino(CasinoLogin cl)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = cl.guid });
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = cl.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "Userloginroulette" });
                return _sqlClientService.Execute("checkFramelogin", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Paymenturltoken(Paymenturltoken cl)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = cl.guid });
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = cl.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "Userloginroulette" });
                return _sqlClientService.Execute("checkFramelogin", ConfigItems.Conn_AccDgroup, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Paymentwith(Paymentwith paymentwith)
        {
            try
            {//@gUserId,@AccNumber,@AccName,@ifsccode,@BankName,@Accounttype,@Amount,@Remark
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gUserId", Value = paymentwith.guid });
                parameters.Add(new SqlParameter() { ParameterName = "AccNumber", Value = paymentwith.anumber });
                parameters.Add(new SqlParameter() { ParameterName = "AccName", Value = paymentwith.aname });
                parameters.Add(new SqlParameter() { ParameterName = "ifsccode", Value = paymentwith.ifsc });
                parameters.Add(new SqlParameter() { ParameterName = "BankName", Value = paymentwith.bname });
                parameters.Add(new SqlParameter() { ParameterName = "Accounttype", Value = paymentwith.atype });
                parameters.Add(new SqlParameter() { ParameterName = "Amount", Value = paymentwith.amt });
                parameters.Add(new SqlParameter() { ParameterName = "Remark", Value = paymentwith.rem });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Insert" });
                DataSet ds = _sqlClientService.Execute("payment_WithDraw", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("Createreq+Royal_WithDraw", JsonConvert.SerializeObject(rWithdraw) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet GetPaymentwith(String uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gUserId", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Getdatabyid" });
                DataSet ds = _sqlClientService.Execute("payment_WithDraw", ConfigItems.Conn_AccDgroup, parameters);
                //_errorLogService.WriteLog("Getreq+Royal_WithDraw", uid.ToString() + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Horsedetail(Horsedetail paymentwith)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = paymentwith.gmid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "selectdetail" });
                DataSet ds = _sqlClientService.Execute("sp_horsedetail", ConfigItems.Conn_Loggameodds, parameters);
                //WriteLogAll("Horsedetail+sp_horsedetail", JsonConvert.SerializeObject(paymentwith) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet PlacebetDconGK(PlacebetDconGK placebet)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters2 = new List<SqlParameter>();
                parameters2.Add(new SqlParameter() { ParameterName = "gameid", Value = placebet.gmid });
                parameters2.Add(new SqlParameter() { ParameterName = "marketid", Value = placebet.mid });
                parameters2.Add(new SqlParameter() { ParameterName = "sectionid", Value = placebet.sid });
                parameters2.Add(new SqlParameter() { ParameterName = "userrate", Value = placebet.urate });
                parameters2.Add(new SqlParameter() { ParameterName = "amount", Value = placebet.amt });
                parameters2.Add(new SqlParameter() { ParameterName = "bettype", Value = placebet.btype });
                parameters2.Add(new SqlParameter() { ParameterName = "partnershiptype", Value = placebet.ptype });
                ds = _sqlClientService.Execute("dim_horseoddsconform", ConfigItems.Conn_Odds, parameters2);
                WriteLogAll("PlacebetDconGK+dim_horseoddsconform", "Req:" + JsonConvert.SerializeObject(placebet) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                WriteLogAll("PlacebetGK" + ex.ToString(), JsonConvert.SerializeObject(placebet));

                throw ex;
            }

        }
        public DataSet GetDepolist(Int64 uid)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "Userid", Value = uid });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "getlist" });
                DataSet ds = _sqlClientService.Execute("getactivetransaction", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("GetDepolist+getactivetransaction", "Req:" + JsonConvert.SerializeObject(uid) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Verifyamt(Couponlist couponlist)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = couponlist.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Accountid", Value = couponlist.aid });
                parameters.Add(new SqlParameter() { ParameterName = "Amount", Value = couponlist.amt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "VerifyAmount" });
                ds = _sqlClientService.Execute("getactivetransaction", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("Couponlist+getactivetransaction", "Req:" + JsonConvert.SerializeObject(couponlist) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet CouponActive(Couponlist couponlist)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = couponlist.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Accountid", Value = couponlist.aid });
                parameters.Add(new SqlParameter() { ParameterName = "Couponcode", Value = couponlist.aid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "couponActive" });
                ds = _sqlClientService.Execute("getactivetransaction", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("CouponActive+getactivetransaction", "Req:" + JsonConvert.SerializeObject(couponlist) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public DataSet Masterexp(Couponlist couponlist)
        {
            try
            {
                DataSet ds = new DataSet();
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "userid", Value = couponlist.uid });
                parameters.Add(new SqlParameter() { ParameterName = "Amount", Value = couponlist.amt });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "masterexposer" });
                ds = _sqlClientService.Execute("getactivetransaction", ConfigItems.Conn_AccDgroup, parameters);
                WriteLogAll("Masterexp+getactivetransaction", "Req:" + JsonConvert.SerializeObject(couponlist) + "Res" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public DataSet LoginVGS(VGSLogin tgs)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "guid", Value = tgs.guid });
                //parameters.Add(new SqlParameter() { ParameterName = "gameid", Value = tgs.gid });
                parameters.Add(new SqlParameter() { ParameterName = "statement", Value = "userlogin" });
                return _sqlClientService.Execute("vivo_login", ConfigItems.Conn_CasinoTP, parameters);
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet PaymenturlNew(Paymenturltoken userLogin)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = userLogin.guid });
                parameters.Add(new SqlParameter() { ParameterName = "gametype", Value = userLogin.gtype });
                parameters.Add(new SqlParameter() { ParameterName = "ptype", Value = userLogin.ptype });
                parameters.Add(new SqlParameter() { ParameterName = "pname", Value = userLogin.pname });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "Userlogin" });
                DataSet ds = _sqlClientService.Execute("payment_domainmaster", ConfigItems.Conn_AccD, parameters);
                WriteLogAll("PaymenturlNew+payment_domainmaster", JsonConvert.SerializeObject(userLogin) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Paymentlst(Paymentlst userLogin)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = userLogin.guid });
                parameters.Add(new SqlParameter() { ParameterName = "type", Value = userLogin.type });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "selectlist" });
                DataSet ds = _sqlClientService.Execute("payment_domainmaster", ConfigItems.Conn_AccD, parameters);
                WriteLogAll("Paymentlst+selectlist", JsonConvert.SerializeObject(userLogin) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet Paymentlstsub(Paymentlst userLogin)
        {
            try
            {
                var parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter() { ParameterName = "UGuid", Value = userLogin.guid });
                parameters.Add(new SqlParameter() { ParameterName = "pname", Value = userLogin.pname });
                parameters.Add(new SqlParameter() { ParameterName = "Statement", Value = "selectlistsub" });
                DataSet ds = _sqlClientService.Execute("payment_domainmaster", ConfigItems.Conn_AccD, parameters);
                WriteLogAll("Paymentlstsub+selectlistsub", JsonConvert.SerializeObject(userLogin) + "Res:" + JsonConvert.SerializeObject(ds));
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
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
