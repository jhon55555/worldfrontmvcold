using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class IPObj
    {
        public string _as { get; set; }
        public string asname { get; set; }
        public string city { get; set; }
        public string country { get; set; }
        public string countryCode { get; set; }
        public string hosting { get; set; }
        public string isp { get; set; }
        public string lat { get; set; }
        public string lon { get; set; }
        public string mobile { get; set; }
        public string org { get; set; }
        public string proxy { get; set; }
        public string query { get; set; }
        public string region { get; set; }
        public string regionName { get; set; }
        public string reverse { get; set; }
        public string status { get; set; }
        public string timezone { get; set; }
        public string zip { get; set; }
    }
}
