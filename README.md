#### Stuck RPC Demo app

First install .NET Core 3.1 SDK, https://docs.microsoft.com/dotnet/core/install/linux-package-managers

Usage:
```
$ git clone https://github.com/xqrzd/kudu-client-net.git
$ cd kudu-client-net
$ git checkout stuck-demo-app
$ cd testapp
$ dotnet run localhost:7051 true
```

The app takes 2 parameters, a comma separated list of masters, and a bool as to whether
2 RPCs should be grouped together or not.
