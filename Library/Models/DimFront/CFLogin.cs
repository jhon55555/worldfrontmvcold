using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class CFLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        [Required(ErrorMessage = "gamename is empty.")]
        public String gamename { get; set; }
    }
    public class CFLoginResp
    {
        public string status { get; set; }
        public string url { get; set; }
        public string desc { get; set; }
    }

    public class DataCF
    {
        public string url { get; set; }
    }

    public class CFUrlResp
    {
        public int status { get; set; }
        public string msg { get; set; }
        public DataCF data { get; set; }
    }
}
