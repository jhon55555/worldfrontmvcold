using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class SSReport
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
    }

    public class Betdetail
    {
        public int batchid { get; set; }
        public string betstartdate { get; set; }
        public string betenddate { get; set; }
        public string accountid { get; set; }
        public string tableid { get; set; }
        public string gameid { get; set; }
        public double betamount { get; set; }
        public double payout { get; set; }
        public string currency { get; set; }
        public string betid { get; set; }
        public string gametype { get; set; }
        public string betspot { get; set; }
        public string betno { get; set; }
        public string betmode { get; set; }
        public string accounttype { get; set; }
        public string gamestarttime { get; set; }
        public string betstatus { get; set; }
        public string gameresult { get; set; }
    }

    public class SSGameTransResp
    {
        public string statuscode { get; set; }
        public string statusmessage { get; set; }
        public List<Betdetail> betdetails { get; set; }
    }
}
