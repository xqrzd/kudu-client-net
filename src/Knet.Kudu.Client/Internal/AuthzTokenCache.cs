using System.Collections.Concurrent;
using Knet.Kudu.Client.Protobuf.Security;

namespace Knet.Kudu.Client.Internal
{
    /// <summary>
    /// Cache for authz tokens received from the master of unbounded capacity. A
    /// client will receive an authz token upon opening a table and put it into the
    /// cache. A subsequent operation that requires an authz token (e.g. writes,
    /// scans) will fetch it from the cache and attach it to the operation request.
    /// </summary>
    public class AuthzTokenCache
    {
        // Map from a table ID to an authz token for that table.
        private readonly ConcurrentDictionary<string, SignedTokenPB> _cache;

        public AuthzTokenCache()
        {
            _cache = new ConcurrentDictionary<string, SignedTokenPB>();
        }

        /// <summary>
        /// Puts the given token into the cache. No validation is done on the validity
        /// or expiration of the token -- that happens on the tablet servers.
        /// </summary>
        /// <param name="tableId">The table ID the authz token is for.</param>
        /// <param name="token">An authz token to put into the cache.</param>
        public void SetAuthzToken(string tableId, SignedTokenPB token)
        {
            _cache[tableId] = token;
        }

        /// <summary>
        /// Returns the cached token for the given 'tableId' if one exists.
        /// </summary>
        /// <param name="tableId">Table ID to get an authz token for.</param>
        public SignedTokenPB GetAuthzToken(string tableId)
        {
            _cache.TryGetValue(tableId, out var token);
            return token;
        }

        /// <summary>
        /// Removes the cached token for the given 'tableId' if one exists.
        /// </summary>
        /// <param name="tableId">Table ID to clear authz token for.</param>
        public void RemoveAuthzToken(string tableId)
        {
            _cache.TryRemove(tableId, out _);
        }
    }
}
