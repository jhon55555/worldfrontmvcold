using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFront
{
    public partial class VGSLogin
    {
        public Int64 uid { get; set; }
        public Guid guid { get; set; }
        //[Required(ErrorMessage = "gid is empty.")]
        //public String gid { get; set; }
        [Required(ErrorMessage = "gname is empty.")]
        public String gname { get; set; }
        public int tid { get; set; }
    }
}
