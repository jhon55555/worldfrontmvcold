using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public class Re1cc
    {
        public Int64 mid { get; set; }
        public String win { get; set; }
    }
    public class Res2cc
    {
        public String cname { get; set; }
    }
    public class Last10ResultCC
    {
        public List<Re1cc> res { get; set; }
        public Res2cc res1 { get; set; }

    }
}
