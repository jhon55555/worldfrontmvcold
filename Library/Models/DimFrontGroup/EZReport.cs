using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class EZReport
    {
        public String guid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
        public Int64 gtid { get; set; }
        public Int64 rid { get; set; }
        public Int64 tid { get; set; }
    }

    public class Datum3
    {
        public string bettypeid { get; set; }
        public string id { get; set; }
        public string roundid { get; set; }
        public object externalroundid { get; set; }
        public string studioid { get; set; }
        public string tableid { get; set; }
        public string uid { get; set; }
        public string operatorid { get; set; }
        public string operatorid2 { get; set; }
        public string sessioncurrency { get; set; }
        public string skinid { get; set; }
        public string betsequenceid { get; set; }
        public string bet { get; set; }
        public string win { get; set; }
        public string gamestring { get; set; }
        public string bankroll { get; set; }
        public string seatid { get; set; }
        public string brandid { get; set; }
        public string rounddatetime { get; set; }
        public string actionid { get; set; }
        public string bettype { get; set; }
        public string platformid { get; set; }
        public string dateinserted { get; set; }
        public string gametypeid { get; set; }
        public object bftransactionfound { get; set; }
        public string gametypename { get; set; }
        public string errorcode { get; set; }
        public string originalerrorcode { get; set; }
        public string transactionid { get; set; }
        public string returnreason { get; set; }
        public object externaltransactionid { get; set; }
    }

    //public class Datum3
    //{
    //    //public string BetTypeID { get; set; }
    //    //public string ID { get; set; }
    //    public string RoundID { get; set; }
    //    //public object ExternalRoundID { get; set; }
    //    //public string StudioID { get; set; }
    //    //public string TableID { get; set; }
    //    public string UID { get; set; }
    //    //public string OperatorID { get; set; }
    //    //public string OperatorID2 { get; set; }
    //    public string SessionCurrency { get; set; }
    //    //public string SkinID { get; set; }
    //    public string BetSequenceID { get; set; }
    //    public string Bet { get; set; }
    //    public string Win { get; set; }
    //    //public string GameString { get; set; }
    //    //public string Bankroll { get; set; }
    //    //public string SeatID { get; set; }
    //    //public string BrandID { get; set; }
    //    public string RoundDateTime { get; set; }
    //    //public string ActionID { get; set; }
    //    public string BetType { get; set; }
    //    //public string PlatformID { get; set; }
    //    //public string DateInserted { get; set; }
    //    public string GameTypeID { get; set; }
    //    //public object BFTransactionFound { get; set; }
    //    public string GameTypeName { get; set; }
    //    //public string ErrorCode { get; set; }
    //    //public string originalErrorCode { get; set; }
    //    public string TransactionID { get; set; }
    //    public string ReturnReason { get; set; }
    //    //public object ExternalTransactionID { get; set; }
    //}

    public class EZGameTransResp
    {
        public string total_rows { get; set; }
        public string page_number { get; set; }
        public string rows_per_page { get; set; }
        public string duration { get; set; }
        public int request_timestamp { get; set; }
        public string request_date { get; set; }
        public string filters { get; set; }
        public string access_list { get; set; }
        public string control { get; set; }
        public List<Datum3> data { get; set; }
        public string dataset { get; set; }
        public string format { get; set; }
        public bool compression { get; set; }
        public string ErrorCode { get; set; }
        public string Error { get; set; }
        public string Details { get; set; }
        public int logid { get; set; }
    }
}
