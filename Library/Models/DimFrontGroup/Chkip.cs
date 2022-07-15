using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{
    public class Chkip
    {
        [Required(ErrorMessage = "ip is empty.")]
        public String ipdemo { get; set; }
    }
}
