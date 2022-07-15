using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public partial class CasinolistRe
    {
        public t2[] t1 { get; set; }
    }

    public partial class t2
    {
        public Int64 clid { get; set; }

        public String gmname { get; set; }
        public String cname { get; set; }

        public Int64 listono { get; set; }
        public Int32 nlunched { get; set; }
        public Int32 m { get; set; }
        public Int32 picon { get; set; }
        public String pid { get; set; }
        public String gtype { get; set; }
    }
}
