using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFrontGroup
{

    public partial class SSLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        [Required(ErrorMessage = "gamename is empty.")]
        public String gamename { get; set; }
        [Required(ErrorMessage = "tableid is empty.")]
        public String tid { get; set; }
        [Required(ErrorMessage = "returnurl is empty.")]
        public String rurl { get; set; }
    }
}
