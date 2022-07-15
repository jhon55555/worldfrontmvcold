using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace Models.DimFront
{
    public class Userexist
    {
        [Required(ErrorMessage = "username is empty.")]
        public String uname { get; set; }
    }
}
