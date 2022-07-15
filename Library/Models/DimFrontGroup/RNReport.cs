using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public partial class RNReport
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "fromdate is empty.")]
        public String fdt { get; set; }
        [Required(ErrorMessage = "todate is empty.")]
        public String tdt { get; set; }
        public String token { get; set; }
    }

    public class GameTransaction
    {
        //public string gameId { get; set; }
        public Double amount { get; set; }
        public string gamename { get; set; }
        public string referenceno { get; set; }
        public string playername { get; set; }
        public string created { get; set; }
        public string txntype { get; set; }
        public string currency { get; set; }
        //public string category { get; set; }
        //public string device { get; set; }
        public string txnid { get; set; }
        //public int playerId { get; set; }
    }

    public class RNGameTransResp
    {
        public DateTime timestamp { get; set; }
        public string hashkey { get; set; }
        public string apikey { get; set; }
        public string sessionkey { get; set; }
        public int apiid { get; set; }
        public List<GameTransaction> gametransactions { get; set; }
        public Error errordetails { get; set; }
    }
}
