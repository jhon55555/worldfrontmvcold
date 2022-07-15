using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class CasinoLogin
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "tableid is empty.")]
        public String tid { get; set; }
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        public String device { get; set; }
    }
}
