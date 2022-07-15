using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public class Main
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int lvl { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string rdesc { get; set; }
        public string remark { get; set; }

    }

    public class SubC
    {
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double bs { get; set; }
        public double l { get; set; }
        public double ls { get; set; }
        public int sr { get; set; }
        public string gstatus { get; set; }
        public int visible { get; set; }
        public int gval { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public string gtype { get; set; }
        public string etype { get; set; }
    }

    public class ChildC
    {
        public Int32 sno { get; set; }
        public Int64 mid { get; set; }
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double l { get; set; }
        public string subtype { get; set; }
        public string gtype { get; set; }
    }

    public class TeenC
    {
        public Main main { get; set; }
        public List<SubC> sub { get; set; }
        public List<ChildC> child { get; set; }
    }
}
