using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public class QTLoginResp
    {
        public string access_token { get; set; }
        public int expires_in { get; set; }
    }
    public class QTLoginResp1
    {
        public string url { get; set; }
    }
}
