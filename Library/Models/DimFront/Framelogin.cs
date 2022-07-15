using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class Framelogin
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        [Required(ErrorMessage = "token is empty.")]
        public String token { get; set; }
    }
}
