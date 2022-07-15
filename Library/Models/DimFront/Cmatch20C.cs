using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public class Mainc
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int lvl { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string rdesc { get; set; }
        public string remark { get; set; }
        public string score { get; set; }

    }

    public class Subc
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


    public class Cmatch20C
    {
        public Mainc main { get; set; }
        public List<Subc> sub { get; set; }
    }

}
