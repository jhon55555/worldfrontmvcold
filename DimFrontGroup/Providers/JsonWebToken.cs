using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace DimFrontGroup.Providers
{
    public class JsonWebToken
    {
        public string AccessToken { get; set; }
        public long Expires { get; set; }
    }
}