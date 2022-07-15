using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class T2
    {
        public Int64 clid { get; set; }

        public String gmname { get; set; }
        public String cname { get; set; }

        public Int64 listono { get; set; }
        public Int32 nlunched { get; set; }

        public String pid { get; set; }
        public String gtype { get; set; }
        public Int32 m { get; set; }
        public Int32 picon { get; set; }
    }

    public class Data1
    {
        public List<T2> t1 { get; set; }

    }

    public class CasinolistRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public Data1 data { get; set; }

    }

}
