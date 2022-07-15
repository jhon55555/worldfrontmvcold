using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Edata1
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

    public class Cdata1
    {
        public string cid { get; set; }
        public string name { get; set; }
        //public IList<Sdata> sdata { get; set; }
        public IList<Edata1> children { get; set; }
    }

    public class Tt1
    {
        public string etid { get; set; }
        public string name { get; set; }
        //public Int32 m { get; set; }
        public IList<Cdata1> children { get; set; }
    }

    public class TreeViewRe
    {
        public IList<Tt1> t1 { get; set; }
    }
}
