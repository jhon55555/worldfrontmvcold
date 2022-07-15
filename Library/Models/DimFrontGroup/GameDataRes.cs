using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Odd1
    {
        public Double odds { get; set; }
        public String otype { get; set; }
        public String oname { get; set; }
        public Double tno { get; set; }
        public Double size { get; set; }
    }

    public class Section1
    {
        public Int64 sid { get; set; }
        public Int32 sno { get; set; }
        public String gstatus { get; set; }
        public String nat { get; set; }
        public Int32 gscode { get; set; }
        public String rem { get; set; } = "";
        public Double max { get; set; }
        public Double min { get; set; }
        public List<Odd1> odds { get; set; }
        public Int32 psrno { get; set; }
    }

    public class Datum2
    {
        public Int64 mid { get; set; }
        public String mname { get; set; }
        public String rem { get; set; } = "";
        public String gtype { get; set; }
        public String status { get; set; }
        public Int32 rc { get; set; }
        public Double maxb { get; set; }
        public Double max { get; set; }
        public Double min { get; set; }
        public Double sno { get; set; }
        public Int32 ocnt { get; set; }
        public Int32 dtype { get; set; }
        public Int32 gscode { get; set; }
        public Boolean iplay { get; set; }
        public Double umaxbof { get; set; }
        public List<Section1> section { get; set; }
    }

    public class GameDataRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public List<Datum2> data { get; set; }
    }
}
