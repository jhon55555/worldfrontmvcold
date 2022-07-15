using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Odd2nnn
    {
        public double b { get; set; }
        public double l { get; set; }
        public string nat { get; set; }
        public int ssid { get; set; }
        public Int32 sno { get; set; }
    }

    public class Subnnn
    {
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double bs { get; set; }
        public double l { get; set; }
        public double ls { get; set; }
        public double bbhav { get; set; }
        public double lbhav { get; set; }
        public int sr { get; set; }
        public string gstatus { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public List<Odd2nnn> odds { get; set; }
        //public string gtype { get; set; }
        public int gval { get; set; }
    }

    public class Data7n
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string remark { get; set; }
        public string rdesc { get; set; }
        public List<Subnnn> sub { get; set; }
    }

    public class VNoteNum
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Data7n data { get; set; }
    }
}
