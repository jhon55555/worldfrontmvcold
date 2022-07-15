using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Re
    {
        public String mid { get; set; }
        public string win { get; set; }
    }
    public class Dataee
    {
        public Res2 res1 { get; set; }
        public List<Re> res { get; set; }
    }
    public class Res2
    {
        public String cname { get; set; }
    }
    public class Last10Result
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Dataee data { get; set; }
    }

}
