using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Odd
    {
        public Double odds { get; set; }
        public String otype { get; set; }
        public String oname { get; set; }
        public Double tno { get; set; }
        public Double size { get; set; }
    }

    public class Section
    {
        public Int64 sid { get; set; }
        public Int32 sno { get; set; }
        public String gstatus { get; set; }
        public Int32 gscode { get; set; }
        public String nat { get; set; }
        public List<Odd> odds { get; set; }

    }

    public class Datum
    {
        public Int64 gmid { get; set; }
        public String ename { get; set; }
        public String etid { get; set; }
        public Boolean iplay { get; set; }
        public String stime { get; set; }
        public Boolean tv { get; set; }
        public Boolean bm { get; set; }
        public Boolean f { get; set; }
        public Boolean f1 { get; set; }
        public Int64 mid { get; set; }
        public String mname { get; set; }
        public String gtype { get; set; }
        public String status { get; set; }
        public Int32 rc { get; set; }
        public Int32 m { get; set; }
        public Int32 gscode { get; set; }
        public List<Section> section { get; set; }

    }

    public class HighlightDataRes
    {
        public int status { get; set; }
        public String msg { get; set; }
        public List<Datum> data { get; set; }

    }


}
