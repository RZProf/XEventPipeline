using System.Text;

namespace XEventPipeline.Setup;

public sealed class AppSettingsFile(string path)
{
    public string Path { get; } = path;

    public static string ResolvePath()
    {
        var workingDirectoryFile = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "appsettings.yml");
        if (File.Exists(workingDirectoryFile)) return workingDirectoryFile;

        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            var projectFile = System.IO.Path.Combine(directory.FullName, "XEventPipeline.csproj");
            var settingsFile = System.IO.Path.Combine(directory.FullName, "appsettings.yml");
            if (File.Exists(projectFile) && File.Exists(settingsFile)) return settingsFile;
        }

        var deployedFile = System.IO.Path.Combine(AppContext.BaseDirectory, "appsettings.yml");
        if (File.Exists(deployedFile)) return deployedFile;

        return workingDirectoryFile;
    }

    public async Task SaveAsync(string contents, CancellationToken cancellationToken = default)
    {
        var directory = System.IO.Path.GetDirectoryName(Path)
                        ?? throw new InvalidOperationException("The configuration file path has no parent directory.");
        var temporaryPath = System.IO.Path.Combine(directory, $".{System.IO.Path.GetFileName(Path)}.{Guid.NewGuid():N}.tmp");

        try
        {
            await File.WriteAllTextAsync(temporaryPath, contents, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), cancellationToken);
            File.Move(temporaryPath, Path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporaryPath)) File.Delete(temporaryPath);
        }
    }
}
