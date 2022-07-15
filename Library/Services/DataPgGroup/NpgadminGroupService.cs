using Common;
using DimFrontOPenser.DataPg;
using Models.DimFrontGroup;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;

namespace Services.DataPgGroup
{
    public class NpgadminGroupService : INpgadminGroupService
    {
        #region Fields

        private readonly IPgsql _sqlClientService;

        #endregion

        #region Ctor

        public NpgadminGroupService(IPgsql sqlClientService)
        {
            _sqlClientService = sqlClientService;
        }

        #endregion

        #region Methods
        public DataSet HighlightDataopen(HighlightDataOpen highlightDataOpen)
        {
            string query = "Select * from " + highlightDataOpen.tablename + " where key='HighlightData_" + highlightDataOpen.etid + "'";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Hlgamede(String keys, String tablename)
        {
            string query = "Select * from " + tablename + " where key in('" + keys + "')";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Hlgamedata(String keys, String tablename)
        {
            string query = "Select * from " + tablename + " where key in('" + keys + "')";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Gamedetail(String gmid, String tablename)
        {
            string query = "Select * from " + tablename + " where key='" + gmid + "'";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Gamedata(String gmid, String tablename)
        {
            string query = "Select * from " + tablename + " where value LIKE '%" + gmid + "%pmid%'";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        public DataSet Treedata(String tablename)
        {
            string query = "Select * from " + tablename + " where key='TreeviewData'";
            var parameters = new List<NpgsqlParameter>();
            DataSet ds = _sqlClientService.Execute(query, ConfigItems.Conn_PgAdmin, parameters);
            //_errorLogService.WriteLog("Framelogin", framelogin + "Res:" + JsonConvert.SerializeObject(ds));
            return ds;
        }
        #endregion
    }
}
