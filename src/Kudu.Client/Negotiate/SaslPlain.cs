using System;
using System.Net;
using System.Text;
using Kudu.Client.Util;

namespace Kudu.Client.Negotiate
{
    public static class SaslPlain
    {
        public static byte[] CreateToken(NetworkCredential credentials)
        {
            var usernameLength = Encoding.UTF8.GetByteCount(credentials.UserName);
            var passwordLength = Encoding.UTF8.GetByteCount(credentials.Password);

            var token = new byte[usernameLength + passwordLength + 2];
            var span = token.AsSpan(1); // Skip authorization identity.

            Encoding.UTF8.GetBytes(credentials.UserName, span);
            Encoding.UTF8.GetBytes(credentials.Password, span.Slice(usernameLength + 1));

            return token;
        }
    }
}
