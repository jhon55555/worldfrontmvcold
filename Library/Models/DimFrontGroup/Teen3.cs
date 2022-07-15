using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Sub13
    {
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double bs { get; set; }
        public double l { get; set; }
        public double ls { get; set; }
        public int sr { get; set; }
        public string gstatus { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public int gval { get; set; }
    }

    public class Data63
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string remark { get; set; }
        public int vv { get; set; }
        public List<Sub13> sub { get; set; }
    }

    public class Teen3
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Data63 data { get; set; }
    }
}
