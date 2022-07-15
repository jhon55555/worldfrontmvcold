using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class OtherCasino
    {
        public Boolean mobile { get; set; }
        [Required(ErrorMessage = "casinotypeid is empty.")]
        public String ctype { get; set; }
    }
}
