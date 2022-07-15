using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{

    public partial class VerifyCode
    {
        public Guid guid { get; set; }
        [Range(1, Int64.MaxValue, ErrorMessage = "code is empty.")]
        public Int64 code { get; set; }
        public String lvlno { get; set; }
    }

    public partial class AuthOnTele
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "password is empty.")]
        public String upass { get; set; }
        public String lvlno { get; set; }
    }
}
