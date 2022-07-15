using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Sub34
    {
        public Int64 sid { get; set; }
        public string nat { get; set; }
        public double b { get; set; }
        public double bs { get; set; }
        public int sr { get; set; }
        public string gstatus { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public string etype { get; set; }
        //public string gtype { get; set; }
        public int gval { get; set; }
    }

    public class Data1211
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string rdesc { get; set; }
        public string remark { get; set; }
        public List<Sub34> sub { get; set; }
    }

    public class Poker20
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Data1211 data { get; set; }
    }




}
