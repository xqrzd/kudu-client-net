using System;
using System.Diagnostics;
using System.IO;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public static class KuduBinaryLocator
    {
        public static string FindBinaryLocation()
        {
            var binPath = Environment.GetEnvironmentVariable("KUDU_BIN_PATH");
            if (binPath != null && Directory.Exists(binPath))
                return binPath;

            // If the `kudu` binary is found on the PATH using `which kudu`, use its parent directory.
            try
            {
                using var process = new Process();
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.FileName = "which";
                process.StartInfo.Arguments = "kudu";
                process.Start();

                // To avoid deadlocks, always read the output stream first and then wait.
                string output = process.StandardOutput.ReadToEnd();
                process.WaitForExit();

                if (process.ExitCode == 0)
                    return Path.GetDirectoryName(output);
                else
                    throw new Exception($"which kudu failed with exit code {process.ExitCode}");
            }
            catch (Exception ex)
            {
                throw new Exception("Error while locating kudu binary", ex);
            }
        }

        public static string FindBinary(string exeName)
        {
            var location = FindBinaryLocation();
            var path = Path.Combine(location, exeName);
            if (File.Exists(path))
                return path;
            else
                throw new FileNotFoundException($"Cannot find executable {exeName} in binary directory {location}");
        }
    }
}
