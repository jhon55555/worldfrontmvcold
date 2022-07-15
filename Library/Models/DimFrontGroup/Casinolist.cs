using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class Casinolist
    {
        [Range(1, Int64.MaxValue, ErrorMessage = "pageicon is empty.")]
        public Int64 picon { get; set; }
    }
    public partial class Casinolistwp
    {
        public t2wp[] t1 { get; set; }
    }

    public partial class t2wp
    {
        public Int64 clid { get; set; }

        public String gmname { get; set; }
        public String cname { get; set; }

        public Int64 listono { get; set; }
        public Int32 nlunched { get; set; }
        public Int32 m { get; set; }
        public Int32 picon { get; set; }
        public String pid { get; set; }
        public String gtype { get; set; }
    }
}
