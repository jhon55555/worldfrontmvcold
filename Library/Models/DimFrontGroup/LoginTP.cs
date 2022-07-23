using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public partial class LoginTP
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        //[Range(1, Int32.MaxValue, ErrorMessage = "casinoid is empty.")]
        public Int32 cid { get; set; }
        //[Range(1, Int32.MaxValue, ErrorMessage = "gameid is empty.")]
        public Int32 gid { get; set; }
        //[Required(ErrorMessage = "tableid is empty.")]
        public String tid { get; set; }
        //[Required(ErrorMessage = "rurl is empty.")]
        public String rurl { get; set; }
        //[Range(0, Int16.MaxValue, ErrorMessage = "istest is empty.")]
        public Int16 istest { get; set; }
        //[Required(ErrorMessage = "device is empty.")]
        public String device { get; set; }
        //[Range(0, Int32.MaxValue, ErrorMessage = "providerid is empty.")]
        public Int32 pid { get; set; }
    }

    public class LoginTPResp
    {
        public int status { get; set; }
        public string msg { get; set; }
        public bool success { get; set; }
        public string data { get; set; }
    }

}
