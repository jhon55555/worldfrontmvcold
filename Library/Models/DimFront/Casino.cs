using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFront
{
    public class Casino
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "webdomain is empty.")]
        [RegularExpression("^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$", ErrorMessage = "invalid domain.")]
        public String webdom { get; set; }
        [Required(ErrorMessage = "ctype is empty.")]
        public String ctype { get; set; }
    }
}
