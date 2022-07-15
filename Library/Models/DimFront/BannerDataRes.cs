using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.DimFront
{
    public class T1
    {
        public Int64 bid { get; set; }
        public String bimg { get; set; }
        public Int64 oid { get; set; }
        public String bdesc { get; set; }

    }

    public class Data
    {
        public List<T1> t1 { get; set; }

    }

    public class BannerDataRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public Data data { get; set; }

    }
    public class PBannerDatareq
    {
        public String dom { get; set; }
    }
}
