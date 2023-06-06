$driver_name=$args[0]
$save_to=$args[1]
$priv_bin_dir=$args[2]

$ADBC_VERSION = "0.4.0"
$ADBC_DRIVER_BASE_URL = "https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-$ADBC_VERSION"
$ADBC_DRIVER_WINDOWS_AMD64_URL = "$ADBC_DRIVER_BASE_URL/adbc_driver_$DRIVER_NAME-$ADBC_VERSION-py3-none-win_amd64.whl"

$driver_triplet = "x86_64-windows-msvc"
$save_as = Join-Path $save_to "$driver_name-$ADBC_VERSION-$driver_triplet.zip"
$unarchive_dir = Join-Path $save_to "$driver_name-$ADBC_VERSION-$driver_triplet"
$dst_filename = Join-Path $priv_bin_dir "libadbc_driver_${driver_name}.dll"

if (-Not(Test-Path -Path $dst_filename -PathType Leaf))
{
    if(-Not(Test-Path -PathType Container -Path $unarchive_dir))
    {
        if (-Not(Test-Path -Path $save_as -PathType Leaf))
        {
            New-Item -Path $save_to -ItemType Container -Force
            Invoke-WebRequest -Uri $ADBC_DRIVER_WINDOWS_AMD64_URL -OutFile $save_as
        }
        Expand-Archive -Path $save_as -DestinationPath $unarchive_dir
    }
    New-Item -Path $priv_lib_dir -ItemType Container -Force
    Copy-Item -Path "$unarchive_dir/adbc_driver_${driver_name}/libadbc_driver_${driver_name}.so" -Destination $dst_filename -Force
}
