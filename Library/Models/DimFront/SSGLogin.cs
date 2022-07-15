using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFront
{
    public partial class SSGLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        [Required(ErrorMessage = "gcat is empty.")]
        public String gcat { get; set; }
        [Required(ErrorMessage = "gname is empty.")]
        public String gname { get; set; }
        [Required(ErrorMessage = "rurl is empty.")]
        public String rurl { get; set; }
    }
}
