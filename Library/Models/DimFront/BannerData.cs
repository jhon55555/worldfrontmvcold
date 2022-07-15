using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class BannerData
    {
        public tb1[] t1 { get; set; }
    }

    public partial class tb1
    {
        public Int32 picon { get; set; }
        public long bid { get; set; }

        public string bimg { get; set; }

        public long oid { get; set; }

        public string bdesc { get; set; }
    }
}
