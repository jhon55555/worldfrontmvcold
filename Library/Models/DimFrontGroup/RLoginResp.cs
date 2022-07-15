using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    //public class RLoginResp
    //{
    //    //public DateTime timeStamp { get; set; }
    //    //public string hashKey { get; set; }
    //    //public string apiKey { get; set; }
    //    public string sessionKey { get; set; }
    //    public string gameUrl { get; set; }
    //    //public int apiId { get; set; }
    //    public Error errorDetails { get; set; }
    //}

    public class Error
    {
        public Int32 errorCode { get; set; }
        public String errorMsg { get; set; }
    }

    public class RLoginResp
    {
        public DateTime timestamp { get; set; }
        public string hashkey { get; set; }
        public string apikey { get; set; }
        public string sessionkey { get; set; }
        public string gameurl { get; set; }
        public int apiid { get; set; }
        public Error errordetails { get; set; }
    }

    public class DataRN
    {
        //public DateTime timeStamp { get; set; }
        //public string hashKey { get; set; }
        //public string apiKey { get; set; }
        public string sessionkey { get; set; }
        public string gameUrl { get; set; }
        //public int apiId { get; set; }
        public object errordetails { get; set; }
    }

    public class RNUrlResp
    {
        public int status { get; set; }
        public string msg { get; set; }
        public DataRN data { get; set; }
    }
}
