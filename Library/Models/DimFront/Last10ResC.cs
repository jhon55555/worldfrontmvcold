using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public class Rec
    {
        public Int64 mid { get; set; }
        public String win { get; set; }
    }
    public class Gc
    {
        public Double p { get; set; }
        public Double b { get; set; }
        public Double t { get; set; }
        public String cname { get; set; }
    }
    public class Last10ResC
    {
        public List<Rec> res { get; set; }
        public Gc g { get; set; }
    }
}
