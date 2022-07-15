using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class T1v
    {
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
        public Int64 gmid { get; set; }
        public string ename { get; set; }
        public int etid { get; set; }
        public int cid { get; set; }
        public string cname { get; set; }
        public bool iplay { get; set; }
        public string gtype { get; set; }
        public bool f { get; set; }
        public bool bm { get; set; }
        public bool tv { get; set; }
        public int scard { get; set; }
        public string stime { get; set; }
        public int m { get; set; }
        public Int64 gtv { get; set; }
        public int iscc { get; set; }
        public String oldgmid { get; set; }
    }

    public class Oddv
    {
        public int psid { get; set; }
        public Int64 sid { get; set; }
        public double odds { get; set; }
        public string otype { get; set; }
        public string oname { get; set; }
        public double tno { get; set; }
        public double size { get; set; }
        public object mid { get; set; }
    }

    public class Sectionv
    {
        public object mid { get; set; }
        public Int64 sid { get; set; }
        public int psid { get; set; }
        public int sno { get; set; }
        public string gstatus { get; set; }
        public string nat { get; set; }
        public int psrno { get; set; }
        public int gscode { get; set; }
        public double min { get; set; }
        public double max { get; set; }
        public string rem { get; set; }
        public List<Oddv> odds { get; set; }
    }

    public class Market
    {
        public object mid { get; set; }
        public string mname { get; set; }
        public string status { get; set; }
        public int rc { get; set; }
        public double sno { get; set; }
        public int dtype { get; set; }
        public int ocnt { get; set; }
        public int visible { get; set; }
        public int gscode { get; set; }
        public string gtype { get; set; }
        public int maxb { get; set; }
        public double max { get; set; }
        public double min { get; set; }
        public string rem { get; set; }
        public List<Sectionv> section { get; set; }
    }

    public class Datav3
    {
        public T1v t1 { get; set; }
        public List<Market> t2 { get; set; }
    }

    public class GetCricketv3data
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Datav3 data { get; set; }
    }


}
