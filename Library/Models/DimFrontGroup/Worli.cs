using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Sub12
    {
        public Int64 sid { get; set; }
        public string gstatus { get; set; }
        public string subtype { get; set; }
        public string etype { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public int gval { get; set; }
        public int sr { get; set; }
    }

    public class Data21
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public string remark { get; set; }
        public List<Sub12> sub { get; set; }
    }

    public class Worli
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Data21 data { get; set; }
    }

}
