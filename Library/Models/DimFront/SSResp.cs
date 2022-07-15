using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class Response
    {
        public string type { get; set; }
        public string username { get; set; }
        public string status { get; set; }
        public string token { get; set; }
        public string errdesc { get; set; }

    }

    public class SSResp
    {
        public Response response { get; set; }
    }
}
