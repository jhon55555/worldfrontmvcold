using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Odd2te
    {
        public double b { get; set; }
        public double l { get; set; }
        public string nat { get; set; }
        public int sid { get; set; }
        public Int32 sno { get; set; }
    }

    public class Sub1te
    {
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double bs { get; set; }
        public double l { get; set; }
        public double ls { get; set; }
        public int visible { get; set; }
        public int sr { get; set; }
        public string gstatus { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public List<Odd2te> odds { get; set; }
        //public string gtype { get; set; }
        public int gval { get; set; }
    }

    public class Data6te
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public int lvl { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string remark { get; set; }
        public List<Sub1te> sub { get; set; }
    }

    public class Teen6
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Data6te data { get; set; }
    }


}
