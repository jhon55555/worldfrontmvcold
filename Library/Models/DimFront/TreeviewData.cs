using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Edata
    {
        public string gmid { get; set; }
        public string name { get; set; }
        public Int64 etid { get; set; }
        public int iscc { get; set; }
    }

    //public class Sdata
    //{
    //    public string stime { get; set; }
    //    public IList<Edata> edata { get; set; }
    //}

    public class Cdata
    {
        public string cid { get; set; }
        public string name { get; set; }
        //public IList<Sdata> sdata { get; set; }
        public IList<Edata> children { get; set; }
    }

    public class T1va
    {
        public string etid { get; set; }
        public string name { get; set; }
        //public Int32 m { get; set; }
        public IList<Cdata> children { get; set; }
    }

    public class TreeviewData
    {
        public IList<T1va> t1 { get; set; }
    }
    public class T1vah
    {
        public string etid { get; set; }
        public string name { get; set; }
        //public Int32 m { get; set; }
        public IList<Edata> children { get; set; }
    }

    public class TreeviewDatahor
    {
        public IList<T1vah> t1 { get; set; }
    }
}
