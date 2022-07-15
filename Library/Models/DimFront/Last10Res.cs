using System;
using System.Collections.Generic;
using System.Text;

namespace Models.DimFront
{
    public class Re1
    {
        public String mid { get; set; }
        public string win { get; set; }
    }
    public class G
    {
        public Double p { get; set; }
        public Double b { get; set; }
        public Double t { get; set; }
        public String cname { get; set; }
    }

    public class Dataee1
    {
        public List<Re1> res { get; set; }
        public G g { get; set; }
    }

    public class Last10Res
    {
        public int status { get; set; }
        public string msg { get; set; }
        public Dataee1 data { get; set; }
    }

}
