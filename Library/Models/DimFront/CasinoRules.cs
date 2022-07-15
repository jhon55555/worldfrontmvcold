using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class CasinoRules
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
    }
}
