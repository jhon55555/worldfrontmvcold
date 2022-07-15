﻿using System;
using System.ComponentModel.DataAnnotations;

namespace Models.DimFrontGroup
{
    public class CasinoTabs
    {
        public Guid guid { get; set; }
        [Required(ErrorMessage = "webdomain is empty.")]
        [RegularExpression("^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$", ErrorMessage = "invalid domain.")]
        public String webdom { get; set; }
        public Int32 ctype { get; set; }
    }
}
