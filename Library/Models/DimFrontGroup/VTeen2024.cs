using System;
using System.Collections.Generic;

namespace Models.DimFrontGroup
{
    public class Sub111ste
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
        //public string gtype { get; set; }
        public string etype { get; set; }
    }

    public class Teen2024ste
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string remark { get; set; }
        public string rdesc { get; set; }
        public List<Sub111ste> sub { get; set; }
    }

    public class VTeen2024
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Teen2024ste data { get; set; }
    }
}
