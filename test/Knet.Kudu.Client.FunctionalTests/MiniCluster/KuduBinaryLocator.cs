using System;
using System.Diagnostics;
using System.IO;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public static class KuduBinaryLocator
    {
        private static readonly string _binaryLocation;
        private static readonly Exception _exception;

        public static string BinaryLocation
        {
            get
            {
                if (_exception != null)
                    throw _exception;

                return _binaryLocation;
            }
        }

        static KuduBinaryLocator()
        {
            var binPath = Environment.GetEnvironmentVariable("KUDU_BIN_PATH");
            if (binPath != null && Directory.Exists(binPath))
            {
                _binaryLocation = binPath;
                return;
            }

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
                    _binaryLocation = Path.GetDirectoryName(output);
                else
                    _exception = new Exception($"which kudu failed with exit code {process.ExitCode}");
            }
            catch (Exception ex)
            {
                _exception = new Exception("Error while locating kudu binary", ex);
            }
        }

        public static string FindBinary(string exeName)
        {
            var location = BinaryLocation;
            var path = Path.Combine(location, exeName);
            if (File.Exists(path))
                return path;
            else
                throw new FileNotFoundException($"Cannot find executable {exeName} in binary directory {location}");
        }
    }
}
