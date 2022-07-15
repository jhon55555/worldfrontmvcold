using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public partial class TPCSUserMaster
    {
        public Int64 uid { get; set; }
        public String guid { get; set; }
        public String uname { get; set; }
        public Decimal gen { get; set; }
        public String curr { get; set; }
        public String webref { get; set; }
        public String webdom { get; set; }
        public String pship { get; set; }
        public Int64 pshiptype { get; set; }
        public String cscode { get; set; }
    }
}
