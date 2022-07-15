using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class Chkip
    {
        [Required(ErrorMessage = "ip is empty.")]
        public String ipdemo { get; set; }
    }
}
