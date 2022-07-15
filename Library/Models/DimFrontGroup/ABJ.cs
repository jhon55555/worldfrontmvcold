using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Subabj
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
        public int gval { get; set; }
    }

    public class Dataabj
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string aall { get; set; }
        public string ball { get; set; }
        public string ares { get; set; }
        public string bres { get; set; }
        public string remark { get; set; }
        public List<Subabj> sub { get; set; }
    }

    public class ABJ
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Dataabj data { get; set; }
    }
}
