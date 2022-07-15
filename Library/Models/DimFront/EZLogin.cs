using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class EZLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        [Required(ErrorMessage = "tableid is empty.")]
        public String tid { get; set; }
    }
}
