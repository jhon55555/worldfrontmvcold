using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class GetTeenOdds
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        public String ip { get; set; }

    }
}
