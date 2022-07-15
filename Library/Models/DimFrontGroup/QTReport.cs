using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class QTReport
    {
        public String guid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
        //public String token { get; set; }
    }

    public class Item
    {
        public string gameid { get; set; }
        public string amount { get; set; }
        public string created { get; set; }
        public string gameprovider { get; set; }
        public string type { get; set; }
        public string gameclienttype { get; set; }
        public string gamecategory { get; set; }
        public string balance { get; set; }
        public string currency { get; set; }
        public string playerdevice { get; set; }
        public string id { get; set; }
        public string operatorid { get; set; }
        public string playergameroundid { get; set; }
        public string playerid { get; set; }
        public string wallettransactionid { get; set; }
        public string roundstatus { get; set; }
        public string jpcontribution { get; set; }
        public string jppayout { get; set; }
    }

    //public class Item
    //{
    //    public string gameId { get; set; }
    //    public string amount { get; set; }
    //    public string created { get; set; }
    //    //public string gameProvider { get; set; }
    //    public string type { get; set; }
    //    //public string gameClientType { get; set; }
    //    //public string gameCategory { get; set; }
    //    public string balance { get; set; }
    //    public string currency { get; set; }
    //    //public string playerDevice { get; set; }
    //    //public string id { get; set; }
    //    //public string operatorId { get; set; }
    //    public string playerGameRoundId { get; set; }
    //    public string playerId { get; set; }
    //    public string walletTransactionId { get; set; }
    //    public string roundStatus { get; set; }
    //    //public string jpContribution { get; set; }
    //    //public string jpPayout { get; set; }
    //}

    public class Link
    {
        public string href { get; set; }
        public string method { get; set; }
        public string rel { get; set; }
        public string name { get; set; }
    }

    public class QTGameTransResp
    {
        public int totalCount { get; set; }
        public List<Item> items { get; set; }
        public List<Link> links { get; set; }
        public string code { get; set; }
        public Details details { get; set; }
        public string message { get; set; }
    }

    public class Details
    {
        public string arg1 { get; set; }
        public string rangeFilter { get; set; }
    }
}
