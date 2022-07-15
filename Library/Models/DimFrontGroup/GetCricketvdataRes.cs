using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Cricketvmain
    {
        public int lt { get; set; }
        public int ft { get; set; }
        public string card { get; set; }
    }
    public class Datacr
    {
        public Cricketvmain t1 { get; set; }
    }

    public class GetCricketvdataRes
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Datacr data { get; set; }
    }
}
