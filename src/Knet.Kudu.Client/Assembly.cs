using System.ComponentModel;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Knet.Kudu.Client.FunctionalTests")]
[assembly: InternalsVisibleTo("Knet.Kudu.Client.Tests")]

#if NET5_0_OR_GREATER

[module: SkipLocalsInit]

#else

namespace System.Runtime.CompilerServices
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit { }
}

#endif
