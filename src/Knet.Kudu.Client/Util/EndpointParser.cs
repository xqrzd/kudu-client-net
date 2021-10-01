using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client.Util;

/// <summary>
/// https://github.com/StackExchange/StackExchange.Redis/blob/master/src/StackExchange.Redis/Format.cs
/// </summary>
public static class EndpointParser
{
    public static bool TryParseInt32([NotNullWhen(true)] string? s, out int value)
    {
        return int.TryParse(s, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out value);
    }

    public static bool TryParse(
        [NotNullWhen(true)] string? addressWithPort,
        int defaultPort,
        [MaybeNullWhen(false)] out HostAndPort result)
    {
        // Adapted from IPEndPointParser in Microsoft.AspNetCore
        // Link: https://github.com/aspnet/BasicMiddleware/blob/f320511b63da35571e890d53f3906c7761cd00a1/src/Microsoft.AspNetCore.HttpOverrides/Internal/IPEndPointParser.cs#L8
        // Copyright (c) .NET Foundation. All rights reserved.
        // Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
        if (string.IsNullOrEmpty(addressWithPort))
        {
            result = null;
            return false;
        }

        string? addressPart;
        string? portPart = null;

        var lastColonIndex = addressWithPort!.LastIndexOf(':');
        if (lastColonIndex > 0)
        {
            // IPv4 with port or IPv6
            var closingIndex = addressWithPort.LastIndexOf(']');
            if (closingIndex > 0)
            {
                // IPv6 with brackets
                addressPart = addressWithPort.Substring(1, closingIndex - 1);
                if (closingIndex < lastColonIndex)
                {
                    // IPv6 with port [::1]:80
                    portPart = addressWithPort.Substring(lastColonIndex + 1);
                }
            }
            else
            {
                // IPv6 without port or IPv4
                var firstColonIndex = addressWithPort.IndexOf(':');
                if (firstColonIndex != lastColonIndex)
                {
                    // IPv6 ::1
                    addressPart = addressWithPort;
                }
                else
                {
                    // IPv4 with port 127.0.0.1:123
                    addressPart = addressWithPort.Substring(0, firstColonIndex);
                    portPart = addressWithPort.Substring(firstColonIndex + 1);
                }
            }
        }
        else
        {
            // IPv4 without port
            addressPart = addressWithPort;
        }

        int? port = null;
        if (portPart is not null)
        {
            if (TryParseInt32(portPart, out var portVal))
            {
                port = portVal;
            }
            else
            {
                // Invalid port, return
                result = null;
                return false;
            }
        }

        result = new HostAndPort(addressPart, port ?? defaultPort);
        return true;
    }
}
