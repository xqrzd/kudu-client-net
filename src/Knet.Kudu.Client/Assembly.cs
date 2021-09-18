using System.ComponentModel;
using System.Runtime.CompilerServices;

#if NET5_0_OR_GREATER

[module: SkipLocalsInit]

#else

namespace System.Runtime.CompilerServices
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit { }
}

#endif
