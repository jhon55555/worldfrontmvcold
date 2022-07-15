using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class UserLogin
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
    public partial class Paymenturltoken
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        public String ptype { get; set; }
        public String pname { get; set; }
    }
    public partial class Paymentlst
    {
        public Guid guid { get; set; }
        public String type { get; set; }
        public String pname { get; set; }
    }
    public partial class Paymentuupdate
    {
        public String guid { get; set; }
        public Int64 uid { get; set; }
        public Int64 curid { get; set; }
    }
}
