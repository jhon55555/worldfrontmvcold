using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Sublt
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
        public double min { get; set; }
        public double max { get; set; }
        public string subtype { get; set; }
        public string gtype { get; set; }
        public int gval { get; set; }
        public string etype { get; set; }
    }

    public class Datlta
    {
        public long mid { get; set; }
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public string gtype { get; set; }
        public object rdesc { get; set; }
        public string remark { get; set; }
        public List<Sublt> sub { get; set; }
    }

    public class Lottcard
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Datlta data { get; set; }
    }


}
