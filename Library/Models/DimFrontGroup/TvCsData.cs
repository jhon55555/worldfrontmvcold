using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class TvCsData
    {
        [Required(ErrorMessage = "gametype is empty.")]
        public String gtype { get; set; }
        [Required(ErrorMessage = "mobiletype is empty.")]
        public String mtype { get; set; }
    }
}
