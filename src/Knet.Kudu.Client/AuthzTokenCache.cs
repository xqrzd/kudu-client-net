using System.Collections.Concurrent;
using Knet.Kudu.Client.Protocol.Security;

namespace Knet.Kudu.Client
{
    public class AuthzTokenCache
    {
        private readonly ConcurrentDictionary<string, SignedTokenPB> _cache;

        public AuthzTokenCache()
        {
            _cache = new ConcurrentDictionary<string, SignedTokenPB>();
        }

        public void SetAuthzToken(string tableId, SignedTokenPB token)
        {
            _cache[tableId] = token;
        }

        public SignedTokenPB GetAuthzToken(string tableId)
        {
            _cache.TryGetValue(tableId, out var token);
            return token;
        }

        public void RemoveAuthzToken(string tableId)
        {
            _cache.TryRemove(tableId, out _);
        }
    }
}
